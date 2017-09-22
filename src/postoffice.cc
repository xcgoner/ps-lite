/**
 *  Copyright (c) 2015 by Contributors
 */
#include <unistd.h>
#include <thread>
#include <chrono>
#include "ps/internal/postoffice.h"
#include "ps/internal/message.h"
#include "ps/base.h"

namespace ps {
Postoffice::Postoffice() {
  std::string van_mode = GetEnvStr("PS_VAN", "zmq");
  // van_ = Van::Create("zmq");
  // van_ = Van::Create("zmqudp");
  van_ = Van::Create(van_mode);
  env_ref_ = Environment::_GetSharedRef();
  const char* val = NULL;
  val = CHECK_NOTNULL(Environment::Get()->find("DMLC_NUM_WORKER"));
  num_workers_ = atoi(val);
  val =  CHECK_NOTNULL(Environment::Get()->find("DMLC_NUM_SERVER"));
  num_servers_ = atoi(val);
  if (van_mode == "zmq") {
    num_keyranges_ = num_servers_;
    chunky_message_ = false;
  }
  else {
    num_keyranges_ = GetEnv("DMLC_NUM_KKERANGE", num_servers_);
    chunky_message_ = true;
  }
  val = CHECK_NOTNULL(Environment::Get()->find("DMLC_ROLE"));
  std::string role(val);
  is_worker_ = role == "worker";
  is_server_ = role == "server";
  is_scheduler_ = role == "scheduler";
  verbose_ = GetEnv("PS_VERBOSE", 0);
}

void Postoffice::Start(const char* argv0, const bool do_barrier) {
  // init glog
  if (argv0) {
    dmlc::InitLogging(argv0);
  } else {
    dmlc::InitLogging("ps-lite\0");
  }

  // init node info.
  for (int i = 0; i < num_workers_; ++i) {
    int id = WorkerRankToID(i);
    for (int g : {id, kWorkerGroup, kWorkerGroup + kServerGroup,
            kWorkerGroup + kScheduler,
            kWorkerGroup + kServerGroup + kScheduler}) {
      node_ids_[g].push_back(id);
    }
  }

  for (int i = 0; i < num_servers_; ++i) {
    int id = ServerRankToID(i);
    for (int g : {id, kServerGroup, kWorkerGroup + kServerGroup,
            kServerGroup + kScheduler,
            kWorkerGroup + kServerGroup + kScheduler}) {
      node_ids_[g].push_back(id);
    }
  }

  for (int g : {kScheduler, kScheduler + kServerGroup + kWorkerGroup,
          kScheduler + kWorkerGroup, kScheduler + kServerGroup}) {
    node_ids_[g].push_back(kScheduler);
  }

  // start van
  van_->Start();

  // record start time
  start_time_ = time(NULL);

  // do a barrier here
  if (do_barrier) Barrier(kWorkerGroup + kServerGroup + kScheduler);
}

void Postoffice::Finalize(const bool do_barrier) {
  if (do_barrier) Barrier(kWorkerGroup + kServerGroup + kScheduler);
  van_->Stop();
  if (exit_callback_) exit_callback_();
}


void Postoffice::AddCustomer(Customer* customer) {
  std::lock_guard<std::mutex> lk(mu_);
  int id = CHECK_NOTNULL(customer)->id();
  CHECK_EQ(customers_.count(id), (size_t)0) << "id " << id << " already exists";
  customers_[id] = customer;
}


void Postoffice::RemoveCustomer(Customer* customer) {
  std::lock_guard<std::mutex> lk(mu_);
  int id = CHECK_NOTNULL(customer)->id();
  customers_.erase(id);
}


Customer* Postoffice::GetCustomer(int id, int timeout) const {
  Customer* obj = nullptr;
  for (int i = 0; i < timeout*1000+1; ++i) {
    {
      std::lock_guard<std::mutex> lk(mu_);
      const auto it = customers_.find(id);
      if (it != customers_.end()) {
        obj = it->second;
        break;
      }
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
  return obj;
}

void Postoffice::Barrier(int node_group) {
  if (GetNodeIDs(node_group).size() <= 1) return;
  auto role = van_->my_node().role;
  if (role == Node::SCHEDULER) {
    CHECK(node_group & kScheduler);
  } else if (role == Node::WORKER) {
    CHECK(node_group & kWorkerGroup);
  } else if (role == Node::SERVER) {
    CHECK(node_group & kServerGroup);
  }

  std::unique_lock<std::mutex> ulk(barrier_mu_);
  barrier_done_ = false;
  Message req;
  req.meta.recver = kScheduler;
  req.meta.request = true;
  req.meta.control.cmd = Control::BARRIER;
  req.meta.control.barrier_group = node_group;
  req.meta.timestamp = van_->GetTimestamp();
  CHECK_GT(van_->Send(req), 0);

  barrier_cond_.wait(ulk, [this] {
      return barrier_done_;
    });
}

const std::vector<Range>& Postoffice::GetServerKeyRanges() {
  if (server_key_ranges_.empty()) {
    range_to_server_map_.clear();
    int server_rank = 0;
    // number of key ranges always geq number of servers
    // mapping: key -> range -> server, to support chunky message
    for (int i = 0; i < num_keyranges_; ++i) {
      server_key_ranges_.push_back(Range(
          kMaxKey / num_keyranges_ * i,
          kMaxKey / num_keyranges_ * (i+1)));
      range_to_server_map_.push_back(server_rank);
      server_rank = server_rank + 1;
      server_rank = server_rank % num_servers_;
    }
  }
  return server_key_ranges_;
}

int Postoffice::RangeToServerRank(int range) {
  // input the rank of the key range
  return range_to_server_map_[range];
}

void Postoffice::Manage(const Message& recv) {
  CHECK(!recv.meta.control.empty());
  const auto& ctrl = recv.meta.control;
  if (ctrl.cmd == Control::BARRIER && !recv.meta.request) {
    barrier_mu_.lock();
    barrier_done_ = true;
    barrier_mu_.unlock();
    barrier_cond_.notify_all();
  }
}

std::vector<int> Postoffice::GetDeadNodes(int t) {
  std::vector<int> dead_nodes;
  if (!van_->IsReady() || t == 0) return dead_nodes;

  time_t curr_time = time(NULL);
  const auto& nodes = is_scheduler_
    ? GetNodeIDs(kWorkerGroup + kServerGroup)
    : GetNodeIDs(kScheduler);
  {
    std::lock_guard<std::mutex> lk(heartbeat_mu_);
    for (int r : nodes) {
      auto it = heartbeats_.find(r);
      if ((it == heartbeats_.end() || it->second + t < curr_time)
            && start_time_ + t < curr_time) {
        dead_nodes.push_back(r);
      }
    }
  }
  return dead_nodes;
}
}  // namespace ps

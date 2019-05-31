/**
 *  Copyright (c) 2015 by Contributors
 */
#ifndef PS_ZMQ_VAN_H_
#define PS_ZMQ_VAN_H_
#include <stdio.h>
#include <stdlib.h>
#include <zmq.h>
#include <string>
#include <cstring>
#include <thread>
#include <cmath>
#include "ps/internal/van.h"
#if _MSC_VER
#define rand_r(x) rand()
#endif

namespace ps {
/**
 * \brief be smart on freeing recved data
 */
inline void FreeData(void* data, void* hint) {
  if (hint == NULL) {
    delete[] static_cast<char*>(data);
  } else {
    delete static_cast<SArray<char>*>(hint);
  }
}

/**
 * \brief ZMQ based implementation
 */
class ZMQVan : public Van {
 public:
  ZMQVan() {}
  virtual ~ZMQVan() {}

 protected:
  void Start(int customer_id) override {
    // start zmq
    start_mu_.lock();
    if (context_ == nullptr) {
      context_ = zmq_ctx_new();
      CHECK(context_ != NULL) << "create 0mq context failed";
    }
    start_mu_.unlock();

    auto val1 = Environment::Get()->find("BYTEPS_ZMQ_MAX_SOCKET");
    int byteps_zmq_max_socket = val1 ? atoi(val1) : 1024;
    zmq_ctx_set(context_, ZMQ_MAX_SOCKETS, byteps_zmq_max_socket);
    PS_VLOG(1) << "BYTEPS_ZMQ_MAX_SOCKET set to " << byteps_zmq_max_socket;

    auto val2 = Environment::Get()->find("BYTEPS_ZMQ_NTHREADS");
    int byteps_zmq_nthreads = val2 ? atoi(val2) : 4;
    zmq_ctx_set(context_, ZMQ_IO_THREADS, byteps_zmq_nthreads);
    PS_VLOG(1) << "BYTEPS_ZMQ_NTHREADS set to " << byteps_zmq_nthreads;

    Van::Start(customer_id);
  }

  void Stop() override {
    PS_VLOG(1) << my_node_.ShortDebugString() << " is stopping";
    Van::Stop();
    // close sockets
    int linger = 0;
    int rc = zmq_setsockopt(receiver_, ZMQ_LINGER, &linger, sizeof(linger));
    CHECK(rc == 0 || errno == ETERM);
    CHECK_EQ(zmq_close(receiver_), 0);
    for (auto& it : senders_) {
      int rc = zmq_setsockopt(it.second, ZMQ_LINGER, &linger, sizeof(linger));
      CHECK(rc == 0 || errno == ETERM);
      CHECK_EQ(zmq_close(it.second), 0);
    }
    senders_.clear();
    zmq_ctx_destroy(context_);
    context_ = nullptr;
  }

  int Bind(const Node& node, int max_retry) override {
    receiver_ = zmq_socket(context_, ZMQ_ROUTER);
    int option = 1;
    CHECK(!zmq_setsockopt(receiver_, ZMQ_ROUTER_MANDATORY, &option, sizeof(option)))
        << zmq_strerror(errno);
    CHECK(receiver_ != NULL)
        << "create receiver socket failed: " << zmq_strerror(errno);
    int local = GetEnv("DMLC_LOCAL", 0);
    std::string hostname = node.hostname.empty() ? "*" : node.hostname;
    int use_kubernetes = GetEnv("DMLC_USE_KUBERNETES", 0);
    if (use_kubernetes > 0 && node.role == Node::SCHEDULER) {
      hostname = "0.0.0.0";
    }
    std::string addr = local ? "ipc:///tmp/" : "tcp://" + hostname + ":";
    int port = node.port;
    unsigned seed = static_cast<unsigned>(time(NULL) + port);
    for (int i = 0; i < max_retry + 1; ++i) {
      auto address = addr + std::to_string(port);
      if (zmq_bind(receiver_, address.c_str()) == 0) break;
      if (i == max_retry) {
        port = -1;
      } else {
        port = 10000 + rand_r(&seed) % 40000;
      }
    }
    switch (node.role) {
      case Node::SCHEDULER:
        LOG(INFO) << "My role is SCHEDULER";
        is_scheduler_ = true;
        is_server_ = false;
        is_worker_ = false;
        break;
      case Node::SERVER:
        LOG(INFO) << "My role is SERVER";
        is_scheduler_ = false;
        is_server_ = true;
        is_worker_ = false;
        break;
      case Node::WORKER:
        LOG(INFO) << "My role is WORKER";
        is_scheduler_ = false;
        is_server_ = false;
        is_worker_ = true;
        break;
      default:
        CHECK(0) << "unknown role: " << node.role;
    }
    return port;
  }

  void Connect(const Node& node) override {
    CHECK_NE(node.id, node.kEmpty);
    CHECK_NE(node.port, node.kEmpty);
    CHECK(node.hostname.size());
    int id = node.id;
    auto it = senders_.find(id);
    if (it != senders_.end()) {
      zmq_close(it->second);
    }
    // worker doesn't need to connect to the other workers. same for server
    if ((node.role == my_node_.role) && (node.id != my_node_.id)) {
      return;
    }
    void* sender = zmq_socket(context_, ZMQ_DEALER);
    CHECK(sender != NULL)
        << zmq_strerror(errno)
        << ". it often can be solved by \"sudo ulimit -n 65536\""
        << " or edit /etc/security/limits.conf";
    if (my_node_.id != Node::kEmpty) {
      std::string my_id = "ps" + std::to_string(my_node_.id);
      zmq_setsockopt(sender, ZMQ_IDENTITY, my_id.data(), my_id.size());
    }
    // connect
    std::string addr =
        "tcp://" + node.hostname + ":" + std::to_string(node.port);
    if (GetEnv("DMLC_LOCAL", 0)) {
      addr = "ipc:///tmp/" + std::to_string(node.port);
    }
    if (zmq_connect(sender, addr.c_str()) != 0) {
      LOG(FATAL) << "connect to " + addr + " failed: " + zmq_strerror(errno);
    }
    senders_[id] = sender;
  }

  int SendMsg(Message& msg) override {
    std::lock_guard<std::mutex> lk(mu_);
    // find the socket
    int id = msg.meta.recver;
    CHECK_NE(id, Meta::kEmpty);
    auto it = senders_.find(id);
    if (it == senders_.end()) {
      LOG(WARNING) << "there is no socket to node " << id;
      return -1;
    }
    void* socket;

    if (msg.meta.simple_app || !msg.meta.control.empty()
        || (GetRoleFromId(id) != Node::WORKER)) {
      socket = it->second;
    }
    else { // GetRoleFromId(id) == Node::WORKER
      socket = receiver_; // scheduler/server --> worker

      // first, send dst id
      std::string dst = "ps" + std::to_string(id);
      int len = dst.size();
      char *dst_array = new char[len + 1];
      strcpy(dst_array, dst.c_str());
      CHECK(dst_array);

      zmq_msg_t zmsg_dstid;
      CHECK_EQ(zmq_msg_init_data(
          &zmsg_dstid, dst_array, len, FreeData, NULL), 0);
      while (true) {
        if (len == zmq_msg_send(&zmsg_dstid, receiver_, ZMQ_SNDMORE)) break;
        if (errno == EINTR) continue;
        CHECK(0) << zmq_strerror(errno);
      }

      // second, send my id
      std::string my_id = "ps" + std::to_string(my_node_.id);
      len = my_id.size();
      char *myid_array = new char[len + 1];
      strcpy(myid_array, my_id.c_str());
      CHECK(myid_array);

      zmq_msg_t zmsg_myid;
      CHECK_EQ(zmq_msg_init_data(
          &zmsg_myid, myid_array, len, FreeData, NULL), 0);
      while (true) {
        if (len == zmq_msg_send(&zmsg_myid, receiver_, ZMQ_SNDMORE)) break;
        if (errno == EINTR) continue;
        CHECK(0) << zmq_strerror(errno);
      }
    }

    // send meta
    int meta_size;
    char* meta_buf = nullptr;
    PackMeta(msg.meta, &meta_buf, &meta_size);
    int tag = ZMQ_SNDMORE | ZMQ_DONTWAIT;
    int n = msg.data.size();
    if (n == 0) tag = 0;
    zmq_msg_t meta_msg;
    zmq_msg_init_data(&meta_msg, meta_buf, meta_size, FreeData, NULL);
    while (true) {
      if (zmq_msg_send(&meta_msg, socket, tag) == meta_size) break;
      if (errno == EINTR) continue;
      CHECK(0) << zmq_strerror(errno);
    }
    // zmq_msg_close(&meta_msg);
    int send_bytes = meta_size;

    // send data
    for (int i = 0; i < n; ++i) {
      zmq_msg_t data_msg;
      SArray<char>* data = new SArray<char>(msg.data[i]);
      int data_size = data->size();
      zmq_msg_init_data(&data_msg, data->data(), data->size(), FreeData, data);
      if (i == n - 1) tag = ZMQ_DONTWAIT;
      while (true) {
        if (zmq_msg_send(&data_msg, socket, tag) == data_size) break;
        if (errno == EINTR) continue;
        LOG(WARNING) << "failed to send message to node [" << id
                     << "] errno: " << errno << " " << zmq_strerror(errno)
                     << ". " << i << "/" << n;
        return -1;
      }
      // zmq_msg_close(&data_msg);
      send_bytes += data_size;
    }
    return send_bytes;
  }

  int RecvMsg(Message* msg) override {
    if (!is_worker_) {
      return CallZmqRecv(receiver_, msg);
    }

    // belows are all for worker
    CHECK(is_worker_);

    int rc;
    size_t n = senders_.size();
    CHECK_GE(n, 1);

    zmq_pollitem_t pollitem[n+1];
    size_t j = 0;
    for (auto& it : senders_) {
      pollitem[j].socket = it.second;
      pollitem[j].events = ZMQ_POLLIN;
      ++j;
    }
    CHECK_EQ(j, n);
    pollitem[n].socket = receiver_;
    pollitem[n].events = ZMQ_POLLIN;

    while (true) {
      std::lock_guard<std::mutex> lk(mu_);
      rc = zmq_poll(pollitem, n + 1, 0);
      CHECK_NE(rc, -1) << zmq_strerror(errno);
      if (rc > 0) break;
    }

    std::vector<void*> socketlist;
    for (size_t k = 0; k < n + 1; ++k) {
      if (pollitem[k].revents & ZMQ_POLLIN)
        socketlist.push_back(pollitem[k].socket);
    }
    CHECK_EQ((int) socketlist.size(), rc);
    // always pick the first one (may optimize this, e.g., load-balanced strategy)
    void *socket = socketlist[0];
    return CallZmqRecv(socket, msg);
  }

 private:
  /**
   * return the node id given the received identity
   * \return -1 if not find
   */
  int GetNodeID(const char* buf, size_t size) {
    if (size > 2 && buf[0] == 'p' && buf[1] == 's') {
      int id = 0;
      size_t i = 2;
      for (; i < size; ++i) {
        if (buf[i] >= '0' && buf[i] <= '9') {
          id = id * 10 + buf[i] - '0';
        } else {
          break;
        }
      }
      if (i == size) return id;
    }
    return Meta::kEmpty;
  }

  int CallZmqRecv(void* socket, Message* msg) {
    msg->data.clear();
    size_t recv_bytes = 0;
    for (int i = 0;; ++i) {
      zmq_msg_t* zmsg = new zmq_msg_t;
      CHECK(zmq_msg_init(zmsg) == 0) << zmq_strerror(errno);
      while (true) {
        std::lock_guard<std::mutex> lk(mu_);
        // the zmq_msg_recv should be non-blocking, otherwise deadlock will happen
        int tag = ZMQ_DONTWAIT;
        if (zmq_msg_recv(zmsg, socket, tag) != -1) break;
        if (errno == EINTR) {
          std::cout << "interrupted";
          continue;
        } else if (errno == EAGAIN) { // ZMQ_DONTWAIT
          continue;
        }
        LOG(WARNING) << "failed to receive message. errno: " << errno << " "
                     << zmq_strerror(errno);
        return -1;
      }
      char* buf = CHECK_NOTNULL((char*)zmq_msg_data(zmsg));
      size_t size = zmq_msg_size(zmsg);
      recv_bytes += size;

      if (i == 0) {
        // identify
        msg->meta.sender = GetNodeID(buf, size);
        msg->meta.recver = my_node_.id;
        CHECK(zmq_msg_more(zmsg));
        zmq_msg_close(zmsg);
        delete zmsg;
      } else if (i == 1) {
        // task
        UnpackMeta(buf, size, &(msg->meta));
        zmq_msg_close(zmsg);
        bool more = zmq_msg_more(zmsg);
        delete zmsg;
        if (!more) break;
      } else {
        // zero-copy
        SArray<char> data;
        data.reset(buf, size, [zmsg, size](char* buf) {
          zmq_msg_close(zmsg);
          delete zmsg;
        });
        msg->data.push_back(data);
        if (!zmq_msg_more(zmsg)) {
          break;
        }
      }
    }
    return recv_bytes;
  }

  Node::Role GetRoleFromId(int id) {
    if (id < 8) return Node::SCHEDULER;
    if (id % 2) return Node::WORKER;
    return Node::SERVER;
  }

  void* context_ = nullptr;
  /**
   * \brief node_id to the socket for sending data to this node
   */
  std::unordered_map<int, void*> senders_;
  std::mutex mu_;
  void* receiver_ = nullptr;

  bool is_scheduler_;
  bool is_server_;
  bool is_worker_;
};
}  // namespace ps

#endif  // PS_ZMQ_VAN_H_

// monitors the liveness other nodes if this is
// a schedule node, or monitors the liveness of the scheduler otherwise
// aliveness monitor
// CHECK(!zmq_socket_monitor(
//     senders_[kScheduler], "inproc://monitor", ZMQ_EVENT_ALL));
// monitor_thread_ = std::unique_ptr<std::thread>(
//     new std::thread(&Van::Monitoring, this));
// monitor_thread_->detach();

// void Van::Monitoring() {
//   void *s = CHECK_NOTNULL(zmq_socket(context_, ZMQ_PAIR));
//   CHECK(!zmq_connect(s, "inproc://monitor"));
//   while (true) {
//     //  First frame in message contains event number and value
//     zmq_msg_t msg;
//     zmq_msg_init(&msg);
//     if (zmq_msg_recv(&msg, s, 0) == -1) {
//       if (errno == EINTR) continue;
//       break;
//     }
//     uint8_t *data = static_cast<uint8_t*>(zmq_msg_data(&msg));
//     int event = *reinterpret_cast<uint16_t*>(data);
//     // int value = *(uint32_t *)(data + 2);

//     // Second frame in message contains event address. it's just the router's
//     // address. no help

//     if (event == ZMQ_EVENT_DISCONNECTED) {
//       if (!is_scheduler_) {
//         PS_VLOG(1) << my_node_.ShortDebugString() << ": scheduler is dead.
//         exit."; exit(-1);
//       }
//     }
//     if (event == ZMQ_EVENT_MONITOR_STOPPED) {
//       break;
//     }
//   }
//   zmq_close(s);
// }

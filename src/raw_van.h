/**
 *  Copyright (c) 2015 by Contributors
 */
 #ifndef PS_ZMQ_VAN_H_
 #define PS_ZMQ_VAN_H_
 #define ZMQ_BUILD_DRAFT_API 1
 #include <zmq.h>
 #include <stdlib.h>
 #include <thread>
 #include <string>
 #include "ps/internal/van.h"
 #include "./meta_generated.h"
 #if _MSC_VER
 #define rand_r(x) rand()
 #endif
 
 #define ZMQ_GROUP_NAME "PS"
 
 namespace ps {
 
 class RAWUDPVan : public Van {
 public:
   RAWUDPVan() { }
  virtual ~RAWUDPVan() { }
 
 protected:
  void Start() override {
    // start zmq
    context_ = zmq_ctx_new();
    CHECK(context_ != NULL) << "create 0mq context failed";
    zmq_ctx_set(context_, ZMQ_MAX_SOCKETS, 65536);
    // TODO: join?
    // zmq_ctx_set(context_, ZMQ_IO_THREADS, 4);
    Van::Start();
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
    zmq_ctx_destroy(context_);
  }
 
  int Bind(const Node& node, int max_retry) override {
     // for UDP, only dish is supported
     receiver_ = zmq_socket(context_, ZMQ_DISH);
     CHECK(receiver_ != NULL)
         << "create dish socket failed: " << zmq_strerror(errno);
     std::string addr = "udp://*:";
     int port = node.port;
     unsigned seed = static_cast<unsigned>(time(NULL)+port);
     for (int i = 0; i < max_retry+1; ++i) {
       auto address = addr + std::to_string(port);
       // for UDP, must join a group
       if (zmq_bind(receiver_, address.c_str()) == 0 && zmq_join(receiver_, ZMQ_GROUP_NAME) == 0) break;
       if (i == max_retry) {
         port = -1;
       } else {
         port = 10000 + rand_r(&seed) % 40000;
       }
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
    if ((node.role == my_node_.role) &&
        (node.id != my_node_.id)) {
      return;
    }
    // for UDP, only radio is supported
    void *sender = zmq_socket(context_, ZMQ_RADIO);
    CHECK(sender != NULL)
        << zmq_strerror(errno)
        << ". it often can be solved by \"sudo ulimit -n 65536\""
        << " or edit /etc/security/limits.conf";
   //  if (my_node_.id != Node::kEmpty) {
   //    std::string my_id = "ps" + std::to_string(my_node_.id);
   //    zmq_setsockopt(sender, ZMQ_IDENTITY, my_id.data(), my_id.size());
   //  }
    // connect
    std::string addr = "udp://" + node.hostname + ":" + std::to_string(node.port);
    // TODO: need checking
    if (GetEnv("DMLC_LOCAL", 0)) {
      addr = "ipc:///tmp/" + std::to_string(node.port);
    }
    if (zmq_connect(sender, addr.c_str()) != 0) {
      LOG(FATAL) <<  "connect to " + addr + " failed: " + zmq_strerror(errno);
    }
    senders_[id] = sender;
  }
 
  int SendMsg(const Message& msg) override {
    std::lock_guard<std::mutex> lk(mu_);
    // find the socket
    int id = msg.meta.recver;
    CHECK_NE(id, Meta::kEmpty);
    auto it = senders_.find(id);
    if (it == senders_.end()) {
      LOG(WARNING) << "there is no socket to node " << id;
      return -1;
    }
    void *socket = it->second;
 
   //  // debug
   //  int buf_size_fb; uint8_t* data_buf_fb;
   //  PackMetaDataFB(msg, my_node_.id, &data_buf_fb, &buf_size_fb);
   //  Message msg_tmp;
   //  UnpackMetaDataFB(data_buf_fb, buf_size_fb, &msg_tmp);
 
    // send meta
    int buf_size; uint8_t* data_buf;
 
   //  PackMetaData(msg, my_node_.id, &data_buf, &buf_size);
   flatbuffers::FlatBufferBuilder* flatbuf_builder = new flatbuffers::FlatBufferBuilder();
   PackMetaDataFB(flatbuf_builder, msg, my_node_.id, &data_buf, &buf_size);
   // LG << "pack size: " << buf_size;
 
   // debug
   // int buf_size_fb; uint8_t* data_buf_fb;
   // PackTestMsg(8, "abc", &data_buf_fb, &buf_size_fb);
   // flatbuffers::FlatBufferBuilder* flatbuf_builder = new flatbuffers::FlatBufferBuilder();
   // PackTestMsg(flatbuf_builder, 8, "abc", &data_buf, &buf_size);
   // LG << "buf_size: " << buf_size;
 
    int tag = 0;
    zmq_msg_t metadata_msg;
   //  zmq_msg_init_data(&metadata_msg, data_buf, buf_size, FreeData, NULL);
    zmq_msg_init_data(&metadata_msg, data_buf, buf_size, FreeDataFB, static_cast<void*>(flatbuf_builder));
    while (true) {
      if (zmq_msg_set_group(&metadata_msg, ZMQ_GROUP_NAME) != 0) {
       LOG(WARNING) << "failed to send message to node [" << id
       << "] errno: " << errno << " " << zmq_strerror(errno);
       return -1;
      }
      if (zmq_msg_send(&metadata_msg, socket, tag) == buf_size) break;
      if (errno == EINTR) continue;
      LOG(WARNING) << "failed to send message to node [" << id
                   << "] errno: " << errno << " " << zmq_strerror(errno);
      return -1;
    }
    zmq_msg_close(&metadata_msg);
 
    return buf_size;
  }
 
  int RecvMsg(Message* msg) override {
    msg->data.clear();
    size_t recv_bytes = 0;
    zmq_msg_t* zmsg = new zmq_msg_t;
    CHECK(zmq_msg_init(zmsg) == 0) << zmq_strerror(errno);
    while (true) {
      if (zmq_msg_recv(zmsg, receiver_, 0) != -1 && strcmp(zmq_msg_group(zmsg), ZMQ_GROUP_NAME) == 0) break;
      if (errno == EINTR) continue;
      LOG(WARNING) << "failed to receive message. errno: "
                   << errno << " " << zmq_strerror(errno);
      return -1;
    }
    uint8_t* buf = CHECK_NOTNULL((uint8_t *)zmq_msg_data(zmsg));
    recv_bytes = zmq_msg_size(zmsg);
   //  LG << "unpack size: " << recv_bytes;
 
   //  UnpackMetaData(buf, recv_bytes, msg);
   UnpackMetaDataFB(buf, recv_bytes, msg);
 
   // // debug
   // UnpackTestMsg(buf, recv_bytes);
 
    zmq_msg_close(zmsg);
    delete zmsg;
    msg->meta.recver = my_node_.id;
    return recv_bytes;
  }
 
 private:
 
  void *context_ = nullptr;
  /**
   * \brief node_id to the socket for sending data to this node
   */
  std::unordered_map<int, void*> senders_;
  std::mutex mu_;
  // as dish
  void *receiver_ = nullptr;
 
 //  // debug
 //  Message msg_tmp;
 
  void PackTestMsg(flatbuffers::FlatBufferBuilder* flatbuf_builder, int num, const std::string& str, uint8_t** data_buf, int* buf_size) {
   
   // preparation
   auto str_str = flatbuf_builder->CreateString(str);
 
   // serialize
   TestMsgBuilder test_msg_builder(*flatbuf_builder);
   test_msg_builder.add_num(num);
   test_msg_builder.add_str(str_str);
 
   auto test_msg = test_msg_builder.Finish();
   // to string
   flatbuf_builder->Finish(test_msg);
   *data_buf = flatbuf_builder->GetBufferPointer();
   *buf_size = flatbuf_builder->GetSize();
  }
 
  void UnpackTestMsg(const uint8_t* data_buf, int buf_size) {
   auto test_msg = GetTestMsg(data_buf);
 
   // deserialize
   LG << "num: " << test_msg->num() << " str: " << test_msg->str()->str();
  }
 
  void PackMetaDataFB(flatbuffers::FlatBufferBuilder* flatbuf_builder, const Message& msg, int sender_id, uint8_t** data_buf, int* buf_size) {
   // convert into flatbuf
 
   // meta
   // not nested
   // preparation
   // node
   std::vector<flatbuffers::Offset<FBNode>> fbnode_vector;
   if (!msg.meta.control.empty()) {
     for (const auto& n : msg.meta.control.node) {
       auto hostname_str = flatbuf_builder->CreateString(n.hostname);
       auto fbnode = CreateFBNode(*flatbuf_builder, n.role, n.id, hostname_str, n.port, n.is_recovery);
       fbnode_vector.push_back(fbnode);
     }
   }
   auto fbnodes = flatbuf_builder->CreateVector(fbnode_vector);
   // body
   // LG << "body size: " << msg.meta.body.size();
   std::string body_string_copy = msg.meta.body;
   auto body_str = flatbuf_builder->CreateString(body_string_copy);
   // data type
   // LG << "pack data_type length: " << msg.meta.data_type.size();
   std::vector<int> data_type_vector;
   for (auto d : msg.meta.data_type) {
     data_type_vector.push_back(d);
   }
   auto data_types = flatbuf_builder->CreateVector(data_type_vector);
   // data
   std::vector<flatbuffers::Offset<flatbuffers::String>> data_vector;
   for (auto d : msg.data) {
     SArray<char>* data = new SArray<char>(d);
     int data_size = data->size();
     auto data_str = flatbuf_builder->CreateString(data->data(), data_size);
     data_vector.push_back(data_str);
   }
   auto data_strs = flatbuf_builder->CreateVector(data_vector);
 
   FBMetaDataBuilder fbmetadata_builder(*flatbuf_builder);
   fbmetadata_builder.add_head(msg.meta.head);
   if (msg.meta.body.size()) {
     fbmetadata_builder.add_body(body_str);
   }
   // control
   if (!msg.meta.control.empty()) {
     fbmetadata_builder.add_cmd(msg.meta.control.cmd);
     fbmetadata_builder.add_node(fbnodes);
     if (msg.meta.control.cmd == Control::BARRIER) {
       fbmetadata_builder.add_barrier_group(msg.meta.control.barrier_group);
     } else if (msg.meta.control.cmd == Control::ACK) {
       fbmetadata_builder.add_msg_sig(msg.meta.control.msg_sig);
     }
   }
   fbmetadata_builder.add_request(msg.meta.request);
   fbmetadata_builder.add_sender_id(sender_id);
   fbmetadata_builder.add_customer_id(msg.meta.customer_id);
   fbmetadata_builder.add_timestamp(msg.meta.timestamp);
   if (msg.meta.data_type.size() > 0) fbmetadata_builder.add_data_type(data_types);
   fbmetadata_builder.add_push(msg.meta.push);
   fbmetadata_builder.add_simple_app(msg.meta.simple_app);
   fbmetadata_builder.add_data(data_strs);
 
   auto fbmetadata = fbmetadata_builder.Finish();
 
   // to string
   flatbuf_builder->Finish(fbmetadata);
   *data_buf = flatbuf_builder->GetBufferPointer();
   *buf_size = flatbuf_builder->GetSize();
 }
 
 void UnpackMetaDataFB(const uint8_t* data_buf, int buf_size, Message* msg) {
   // to flatbuf
   auto fbmetadata = GetFBMetaData(data_buf);
 
   // to meta
   msg->meta.head = fbmetadata->head();
   if (fbmetadata->body() != NULL) msg->meta.body = fbmetadata->body()->c_str();
   if (fbmetadata->node() != NULL) {
     msg->meta.control.cmd = static_cast<Control::Command>(fbmetadata->cmd());
     msg->meta.control.barrier_group = fbmetadata->barrier_group();
     msg->meta.control.msg_sig = fbmetadata->msg_sig();
     for (int i = 0; i < fbmetadata->node()->Length(); ++i) {
       auto p = fbmetadata->node()->Get(i);
       Node n;
       n.role = static_cast<Node::Role>(p->role());
       n.port = p->port();
       n.hostname = p->hostname()->c_str();
       n.id = p->id();
       n.is_recovery = p->is_recovery();
       msg->meta.control.node.push_back(n);
     }
   } else {
     msg->meta.control.cmd = Control::EMPTY;
   }
   msg->meta.request = fbmetadata->request();
   msg->meta.sender = fbmetadata->sender_id();
   msg->meta.customer_id = fbmetadata->customer_id();
   msg->meta.timestamp = fbmetadata->timestamp();  
   if (fbmetadata->data_type() != NULL) {
     msg->meta.data_type.resize(fbmetadata->data_type()->Length());
     for (int i = 0; i < fbmetadata->data_type()->Length(); ++i) {
       msg->meta.data_type[i] = static_cast<DataType>(fbmetadata->data_type()->Get(i));
     }
   }
   msg->meta.push = fbmetadata->push();
   msg->meta.simple_app = fbmetadata->simple_app();
 
   // data
   for (int i = 0; i < fbmetadata->data()->Length(); ++i) {
     // TODO: zero-copy
     SArray<char> data;
     data.CopyFrom(fbmetadata->data()->Get(i)->c_str(), fbmetadata->data()->Get(i)->str().size());
     msg->data.push_back(data);
   }
 }
 
 }; //RAWUDPVan
 
 
 
 }  // namespace ps
 
 #endif  // PS_RAW_VAN_H_
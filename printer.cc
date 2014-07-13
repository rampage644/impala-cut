#include <iostream>
#include <netinet/in.h>

#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/protocol/TDebugProtocol.h>
#include <thrift/transport/TFileTransport.h>
#include "be/generated-sources/gen-cpp/ImpalaInternalService_types.h"

int main(int argc, char** argv) {
  if (argc < 2)
  {
    std::cerr << "No filename found" << std::endl;
    return 1;
  }
  using namespace apache::thrift::transport;
  using namespace apache::thrift::protocol;
  using namespace apache::thrift;
  impala::TExecPlanFragmentParams req;

  boost::shared_ptr<TFileTransport> transport(new TFileTransport(argv[1]));
  boost::shared_ptr<TBinaryProtocol> proto(new TBinaryProtocol(transport));
  req.read(proto.get());
  std::cout << ThriftDebugString(req) << std::endl;
  return 0;
}
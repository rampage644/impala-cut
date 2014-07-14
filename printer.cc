#include <iostream>
#include <string>
#include <netinet/in.h>

#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/protocol/TDebugProtocol.h>
#include <thrift/transport/TFileTransport.h>
#include "be/generated-sources/gen-cpp/ImpalaInternalService_types.h"
#include "be/generated-sources/gen-cpp/Frontend_types.h"



template <class T>
void dumps(const char* filename) 
{
  using namespace apache::thrift::transport;
  using namespace apache::thrift::protocol;
  using namespace apache::thrift;
  T obj; 

  boost::shared_ptr<TFileTransport> transport(new TFileTransport(filename));
  boost::shared_ptr<TBinaryProtocol> proto(new TBinaryProtocol(transport));
  obj.read(proto.get());
  std::cout << ThriftDebugString(obj) << std::endl;
}

int main(int argc, char** argv) {
  std::string classname = "TExecPlanFragmentParams";
  if (argc < 3)
  {
    std::cerr << "Not enough argv" << std::endl;
    return 1;
  }


  if (!strcmp(argv[2], "TExecPlanFragmentParams")) 
    dumps<impala::TExecPlanFragmentParams>(argv[1]);
  else if (!strcmp(argv[2], "TQueryExecRequest"))
    dumps<impala::TQueryExecRequest>(argv[1]);
  else if (!strcmp(argv[2], "TQueryOptions"))
    dumps<impala::TQueryOptions>(argv[1]);

  return 1;
}


#include <cstdint>
#include "Types.hpp"

#include "tableLoader.cpp"
#include <iostream>
#include <cstdio>
#include <cstdlib>
#include <ctime>

using namespace std;

void joinQuery() {
std::cout<<"Inside joinQuery\n";
  vector<uint64_t> temp_cust;
  vector<uint64_t> temp_ord;
  vector<uint64_t> temp_ol;
  
  for(uint64_t x1=0; x1 < t_customer.size(); x1++) {
      if(t_customer[x1].c_last.value[0] == 'B') {
          temp_cust.push_back(x1);
      }
  }

cout<<"x1 done\n";

  for(uint64_t x2 = 0; x2 < t_order.size(); x2++) {
      for(uint64_t y2 = 0; y2 < temp_cust.size(); y2++) {
          if(t_customer[temp_cust[y2]].c_id == t_order[x2].o_c_id) {
              temp_ord.push_back(x2);
          }
      }
  }

cout<<"One loop x2 y2 done\n";
  
  for(uint64_t x3 = 0; x3 < t_orderline.size(); x3++) {
      for (uint64_t y3 = 0; y3 < temp_ord.size(); y3++) {
          if((t_order[temp_ord[y3]].o_w_id == t_orderline[x3].ol_w_id) && (t_order[temp_ord[y3]].o_d_id == t_orderline[x3].ol_d_id) && (t_order[temp_ord[y3]].o_id == t_orderline[x3].ol_o_id)){
              temp_ol.push_back(x3);
          }
      }
  }

cout<<"One loop x3 y3 done\n";
  
  uint64_t sum = 0;
  for(uint64_t x4 = 0; x4 < temp_ol.size(); x4++) {
      sum = sum + (uint64_t)t_orderline[temp_ol[x4]].ol_quantity.value * (uint64_t)t_orderline[temp_ol[x4]].ol_amount.value;
  }

cout<<"Result of query is: Sum = "<<sum<<endl;
  
  return;
}

int main() {
  load();
  
  Timestamp now;
  uint64_t start, end;
  start = time(NULL);
  std::cout<<"start = "<<start<<endl;

  // Calling joinQuery() 10 times in sequence.
  joinQuery();
  end = time(NULL);
  std::cout<<"end = "<<end<<endl;
  std::cout<<"Total time taken = "<<(end - start)<<" seconds"<<endl;
  
  return 0;
}

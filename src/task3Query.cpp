#include <cstdint>
//#include "Types.hpp"

#include "tableLoader.cpp"
//#include <iostream>
//#include <cstdio>
#include <cstdlib>
#include <ctime>
#include <unordered_map>

using namespace std;

void joinQuery() {
std::cout<<"Inside joinQuery\n";
  std::unordered_map<tuple<Integer, Integer, Integer>, uint64_t> uMapCust;
  std::unordered_map<tuple<Integer, Integer, Integer>, uint64_t> uMapOrder;
	
  struct customer cust;
  for(uint64_t x1=0; x1 < t_customer.size(); x1++) {
      cust = t_customer[x1];
      if(cust.c_last.value[0] == 'B') {	      
          //temp_cust.push_back(x1);
		  uMapCust.insert(std::pair<tuple<Integer, Integer, Integer>, uint64_t>(std::tuple<Integer, Integer, Integer>(cust.c_w_id, cust.c_d_id, cust.c_id), x1));
      }
  }
cout<<"Loop x1 done\n";
		  
  struct order odr;
  for(uint64_t x2 = 0; x2 < t_order.size(); x2++) {
      odr = t_order[x2];
	  if(uMapCust.count(std::tuple<Integer, Integer, Integer>(odr.o_w_id, odr.o_d_id, odr.o_c_id)) == 1) {
	      uMapOrder.insert(std::pair<tuple<Integer, Integer, Integer>, uint64_t>(std::tuple<Integer, Integer, Integer>(odr.o_w_id, odr.o_d_id, odr.o_id), x2));
	  }
/*      for(uint64_t y2 = 0; y2 < temp_cust.size(); y2++) {
          if(t_customer[temp_cust[y2]].c_id == t_order[x2].o_c_id) {
              temp_ord.push_back(x2);
          }
      }*/
  }
cout<<"Loop x2 done\n";

  uint64_t sum = 0;  
  struct orderline ol;
  for(uint64_t x3 = 0; x3 < t_orderline.size(); x3++) {
      ol = t_orderline[x3];
      if(uMapOrder.count(std::tuple<Integer, Integer, Integer>(ol.ol_w_id, ol.ol_d_id, ol.ol_o_id)) == 1) {
	      sum = sum + ((uint64_t)ol.ol_quantity.value * (uint64_t)ol.ol_amount.value);
	  }
/*      for (uint64_t y3 = 0; y3 < temp_ord.size(); y3++) {
          if((t_order[temp_ord[y3]].o_w_id == t_orderline[x3].ol_w_id) && (t_order[temp_ord[y3]].o_d_id == t_orderline[x3].ol_d_id) && (t_order[temp_ord[y3]].o_id == t_orderline[x3].ol_o_id)){
              temp_ol.push_back(x3);
          }
      }*/
  }
  cout<<"Result of query is: Sum = "<<sum<<endl;
cout<<"Loop x3 done\n";

/*
  for(uint64_t x4 = 0; x4 < temp_ol.size(); x4++) {
      sum = sum + (uint64_t)t_orderline[temp_ol[x4]].ol_quantity.value * (uint64_t)t_orderline[temp_ol[x4]].ol_amount.value;
  }
*/

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

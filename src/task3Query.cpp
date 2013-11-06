#include <cstdint>
//#include "Types.hpp"

#include "tableLoader.cpp"
//#include <iostream>
//#include <cstdio>
#include <cstdlib>
#include <chrono>
#include <map>

using namespace std;
using namespace std::chrono;

Numeric<6,2> joinQuery() {
  std::map<tuple<Integer, Integer, Integer>, uint64_t> uMapCust;
  std::map<tuple<Integer, Integer, Integer>, uint64_t> uMapOrder;
	
  struct customer cust;
  for(uint64_t x1=0; x1 < t_customer.size(); x1++) {
      cust = t_customer[x1];
      if(cust.c_last.value[0] == 'B') {
		  uMapCust.insert(std::pair<tuple<Integer, Integer, Integer>, uint64_t>(std::tuple<Integer, Integer, Integer>(cust.c_w_id, cust.c_d_id, cust.c_id), x1));
      }
  }
		  
  struct order odr;
  for(uint64_t x2 = 0; x2 < t_order.size(); x2++) {
      odr = t_order[x2];
	  if(uMapCust.count(std::tuple<Integer, Integer, Integer>(odr.o_w_id, odr.o_d_id, odr.o_c_id)) == 1) {
	      uMapOrder.insert(std::pair<tuple<Integer, Integer, Integer>, uint64_t>(std::tuple<Integer, Integer, Integer>(odr.o_w_id, odr.o_d_id, odr.o_id), x2));
	  }
  }

  Numeric<6,2> sum = 0;
  struct orderline ol;
  for(uint64_t x3 = 0; x3 < t_orderline.size(); x3++) {
      ol = t_orderline[x3];
      if(uMapOrder.count(std::tuple<Integer, Integer, Integer>(ol.ol_w_id, ol.ol_d_id, ol.ol_o_id)) == 1) {
	      sum = sum + (ol.ol_quantity.value * ol.ol_amount.value);
	  }
  }
  
  return sum;
}

int main() {
  load();  // Loading all tables.

  // Calling joinQuery() 10 times in sequence.
  for(int counter = 0; counter < 10; counter++) {
    std::cout<<"Calling "<<counter+1<<"th time\n";
	auto start=high_resolution_clock::now();
	cout<<"Result of query is: Sum = "<<joinQuery()<<endl;
	cout << "Time of tas3Query " << counter+1 << " call = " << duration_cast<duration<double>>(high_resolution_clock::now()-start).count() << "s" << endl << endl;
  }
  
  return 0;
}

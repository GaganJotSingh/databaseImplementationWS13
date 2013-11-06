#include <cstdint>
#include <cstdlib>
#include <chrono>
#include <ctime>
#include <map>
#include <atomic>
#include <signal.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

#include "tableLoader.cpp"

using namespace std;
using namespace std::chrono;

const int32_t warehouses=5;

int32_t urand(int32_t min,int32_t max) {
   return (random()%(max-min+1))+min;
}

int32_t urandexcept(int32_t min,int32_t max,int32_t v) {
   if (max<=min)
      return min;
   int32_t r=(random()%(max-min))+min;
   if (r>=v)
      return r+1; else
      return r;
}

int32_t nurand(int32_t A,int32_t x,int32_t y) {
   return ((((random()%A)|(random()%(y-x+1)+x))+42)%(y-x+1))+x;
}

void newOrderRandom(Timestamp now) {
   int32_t w_id=urand(1,warehouses);
   int32_t d_id=urand(1,1);
   int32_t c_id=nurand(1023,1,3000);
   int32_t ol_cnt=urand(5,15);

   int32_t supware[15];
   int32_t itemid[15];
   int32_t qty[15];
   for (int32_t i=0; i<ol_cnt; i++) {
      if (urand(1,100)>1)
         supware[i]=w_id; else
         supware[i]=urandexcept(1,warehouses,w_id);
      itemid[i]=nurand(8191,1,100000);
      qty[i]=urand(1,10);
   }

   newOrder(w_id,d_id,c_id,ol_cnt,supware,itemid,qty,now);
}

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

atomic<bool> childRunning;

static void SIGCHLD_handler(int /*sig*/) {
   int status;
   pid_t childPid=wait(&status);
   // now the child with process id childPid is dead
   childRunning=false;
}

int main() {
   load();      //Loading all tables.
   
   struct sigaction sa;
   sigemptyset(&sa.sa_mask);
   sa.sa_flags=0;
   sa.sa_handler=SIGCHLD_handler;
   sigaction(SIGCHLD,&sa,NULL);
   
   childRunning = true;
   pid_t pid = fork();
   
   if (pid) {                // parent
     while (childRunning);   // wait for child
	 Timestamp now;
     now.value = time(NULL);
     newOrderRandom(now);
   } else if(pid == 0){                  // forked child
     cout << "Result of query is: Sum = " << joinQuery() << endl;
	 return 0;               // child is finished
   } else {                  // pid < 0; failed to fork
     cerr << "Failed to fork" << endl;
     exit(1);
   }
   
   return 0;
}

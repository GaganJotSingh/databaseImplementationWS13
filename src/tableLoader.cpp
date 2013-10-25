#include "Types.cpp"
#include "schema.cpp"
#include <iostream>
#include <cstring>
#include <cstdio>

using namespace std;

// Data parser for table warehouse: loads data from tpcc_warehouse.tbl file to t_warehouse vector.
void load_warehouse() {
  char w_id[512];
  char w_name[11];
  char w_street_1[21];
  char w_street_2[21];
  char w_city[21];
  char w_state[3];
  char w_zip[10];
  char w_tax[10];
  char w_ytd[16];
  
  char buff[1024];
  struct warehouse wh;
  FILE *pFile = fopen("tpcc_warehouse.tbl", "r");
  uint64_t counter = 0;
  
  while(fgets(buff, 1024, pFile) != NULL) {
//    std::cout<<"line = "<<buff<<endl;
	
	sscanf(buff, "%[^|]|%[^|]|%[^|]|%[^|]|%[^|]|%[^|]|%[^|]|%[^|]|%s", w_id, w_name, w_street_1, w_street_2, w_city, w_state, w_zip, w_tax, w_ytd);
	
//    std::cout<<w_id<<" "<<w_name<<" "<<w_street_1<<" "<<w_street_2<<" "<<w_city<<" "<<w_state<<" "<<w_zip<<" "<<w_tax<<" "<<w_ytd<<endl;
	
	wh.w_id = Integer::castString(w_id, strlen(w_id));
	wh.w_name = Varchar<10>::castString(w_name, strlen(w_name));
	wh.w_street_1 = Varchar<20>::castString(w_street_1, strlen(w_street_1));
	wh.w_street_2 = Varchar<20>::castString(w_street_2, strlen(w_street_2));
	wh.w_city = Varchar<20>::castString(w_city, strlen(w_city));
	wh.w_state = Char<2>::castString(w_state, strlen(w_state));
	wh.w_zip = Char<9>::castString(w_zip, strlen(w_zip));
	wh.w_tax = Numeric<4,4>::castString(w_tax, strlen(w_tax));
	wh.w_ytd = Numeric<12,2>::castString(w_ytd, strlen(w_ytd));
	t_warehouse.push_back(wh);
	
	map_warehouse.insert(std::pair<Integer, uint64_t>(wh.w_id, counter));
	counter += 1;
  }
  
  fclose (pFile);
}

// Data parser for table district: loads data from tpcc_district.tbl file to t_district vector.
void load_district() {
  char d_id[512];
  char d_w_id[512];
  char d_name[11];
  char d_street_1[21];
  char d_street_2[21];
  char d_city[21];
  char d_state[3];
  char d_zip[10];
  char d_tax[10];
  char d_ytd[16];
  char d_next_o_id[512];
  
  char buff[1024];
  struct district dt;
  FILE *pFile = fopen("tpcc_district.tbl", "r");
  uint64_t counter = 0;
  
  while(fgets(buff, 1024, pFile) != NULL) {
//    std::cout<<"line = "<<buff<<endl;
	
	sscanf(buff, "%[^|]|%[^|]|%[^|]|%[^|]|%[^|]|%[^|]|%[^|]|%[^|]|%[^|]|%[^|]|%s", d_id, d_w_id, d_name, d_street_1, d_street_2, d_city, d_state, d_zip, d_tax, d_ytd, d_next_o_id);
	
//    std::cout<<d_id<<" "<<d_w_id<<" "<<d_name<<" "<<d_street_1<<" "<<d_street_2<<" "<<d_city<<" "<<d_state<<" "<<d_zip<<" "<<d_tax<<" "<<d_ytd<<" "<<d_next_o_id<<endl;
	
	dt.d_id = Integer::castString(d_id, strlen(d_id));
	dt.d_w_id = Integer::castString(d_w_id, strlen(d_w_id));
	dt.d_name = Varchar<10>::castString(d_name, strlen(d_name));
	dt.d_street_1 = Varchar<20>::castString(d_street_1, strlen(d_street_1));
	dt.d_street_2 = Varchar<20>::castString(d_street_2, strlen(d_street_2));
	dt.d_city = Varchar<20>::castString(d_city, strlen(d_city));
	dt.d_state = Char<2>::castString(d_state, strlen(d_state));
	dt.d_zip = Char<9>::castString(d_zip, strlen(d_zip));
	dt.d_tax = Numeric<4,4>::castString(d_tax, strlen(d_tax));
	dt.d_ytd = Numeric<12,2>::castString(d_ytd, strlen(d_ytd));
    dt.d_next_o_id = Integer::castString(d_next_o_id, strlen(d_next_o_id));
	t_district.push_back(dt);
	
	map_district.insert(std::pair<tuple<Integer, Integer>, uint64_t>(std::tuple<Integer, Integer>(dt.d_w_id, dt.d_id), counter));
	counter += 1;
  }
  
  fclose (pFile);
}

// Data parser for table customer: loads data from tpcc_customer.tbl file to t_customer vector.
void load_customer() {   		
  char c_id[512];
  char c_d_id[512];
  char c_w_id[512];
  char c_first[17];
  char c_middle[3];
  char c_last[17];
  char c_street_1[21];
  char c_street_2[21];
  char c_city[21];
  char c_state[3];
  char c_zip[10];
  char c_phone[17];
  char c_since[512];
  char c_credit[3];
  char c_credit_lim[16];
  char c_discount[10];
  char c_balance[16];
  char c_ytd_paymenr[16];
  char c_payment_cnt[6];
  char c_delivery_cnt[6];
  char c_data[501];
  
  char buff[1024];
  struct customer cust;
  FILE *pFile = fopen("tpcc_customer.tbl", "r");
  uint64_t counter = 0;
  
  while(fgets(buff, 1024, pFile) != NULL) {
//    std::cout<<"line = "<<buff<<endl;
	
	sscanf(buff, "%[^|]|%[^|]|%[^|]|%[^|]|%[^|]|%[^|]|%[^|]|%[^|]|%[^|]|%[^|]|%[^|]|%[^|]|%[^|]|%[^|]|%[^|]|%[^|]|%[^|]|%[^|]|%[^|]|%[^|]|%s", c_id, c_d_id, c_w_id, c_first, c_middle, c_last, c_street_1, c_street_2, c_city, c_state, c_zip, c_phone, c_since, c_credit, c_credit_lim, c_discount, c_balance, c_ytd_paymenr, c_payment_cnt, c_delivery_cnt, c_data);
	
//    std::cout<<c_id<<" "<<c_d_id<<" "<<c_w_id<<" "<<c_first<<" "<<c_middle<<" "<<c_last<<" "<<c_street_1<<" "<<c_street_2<<" "<<c_city<<" "<<c_state<<" "<<c_zip<<" "<<c_phone<<" "<<c_since<<" "<<c_credit<<" "<<c_credit_lim<<" "<<c_discount<<" "<<c_balance<<" "<<c_ytd_paymenr<<" "<<c_payment_cnt<<" "<<c_delivery_cnt<<" "<<c_data<<endl;

	cust.c_id = Integer::castString(c_id, strlen(c_id));
	cust.c_d_id = Integer::castString(c_d_id, strlen(c_d_id));
	cust.c_w_id = Integer::castString(c_w_id, strlen(c_w_id));
	cust.c_first = Varchar<16>::castString(c_first, strlen(c_first));
	cust.c_middle = Char<2>::castString(c_middle, strlen(c_middle));
	cust.c_last = Varchar<16>::castString(c_last, strlen(c_last));
	cust.c_street_1 = Varchar<20>::castString(c_street_1, strlen(c_street_1));
	cust.c_street_2 = Varchar<20>::castString(c_street_2, strlen(c_street_2));
	cust.c_city = Varchar<20>::castString(c_city, strlen(c_city));
	cust.c_state = Char<2>::castString(c_state, strlen(c_state));
	cust.c_zip = Char<9>::castString(c_zip, strlen(c_zip));
	cust.c_phone = Char<16>::castString(c_phone, strlen(c_phone));
	cust.c_since = Timestamp::castString(c_since, strlen(c_since));
    cust.c_credit = Char<2>::castString(c_credit, strlen(c_credit));
    cust.c_credit_lim = Numeric<12,2>::castString(c_credit_lim, strlen(c_credit_lim));
    cust.c_discount = Numeric<4,4>::castString(c_discount, strlen(c_discount));
    cust.c_balance = Numeric<12,2>::castString(c_balance, strlen(c_balance));
    cust.c_ytd_paymenr = Numeric<12,2>::castString(c_ytd_paymenr, strlen(c_ytd_paymenr));
    cust.c_payment_cnt = Numeric<4,0>::castString(c_payment_cnt, strlen(c_payment_cnt));
    cust.c_delivery_cnt = Numeric<4,0>::castString(c_delivery_cnt, strlen(c_delivery_cnt));
    cust.c_data = Varchar<500>::castString(c_data, strlen(c_data));
	t_customer.push_back(cust);
	
	map_customer.insert(std::pair<tuple<Integer, Integer, Integer>, uint64_t>(std::tuple<Integer, Integer, Integer>(cust.c_w_id, cust.c_d_id, cust.c_id), counter));
	
	customer_wdl.insert(std::pair<uint64_t, uint64_t>(hashKey<Integer, Integer, Varchar<16>, Varchar<16>>(cust.c_w_id, cust.c_d_id, cust.c_last, cust.c_first), counter));
	
//	customer_wdl.insert(std::pair<tuple<Integer, Integer, Varchar<16>, Varchar<16>>, uint64_t>(std::tuple<Integer, Integer, Varchar<16>, Varchar<16>>(cust.c_w_id, cust.c_d_id, cust.c_last, cust.c_first), counter));
	counter += 1;
  }
  
  fclose (pFile);
}

// Data parser for table history: loads data from tpcc_history.tbl file to t_history vector.
void load_history() {
  char h_c_id[512];
  char h_c_d_id[512];
  char h_c_w_id[512];
  char h_d_id[512];
  char h_w_id[512];
  char h_date[512];
  char h_amount[10];
  char h_data[25];
  
  char buff[1024];
  struct history hist;
  FILE *pFile = fopen("tpcc_history.tbl", "r");
  
  while(fgets(buff, 1024, pFile) != NULL) {
//    std::cout<<"line = "<<buff<<endl;
	
	sscanf(buff, "%[^|]|%[^|]|%[^|]|%[^|]|%[^|]|%[^|]|%[^|]|%s", h_c_id, h_c_d_id, h_c_w_id, h_d_id, h_w_id, h_date, h_amount, h_data);

//    std::cout<<h_c_id<<" "<<h_c_d_id<<" "<<h_c_w_id<<" "<<h_d_id<<" "<<h_w_id<<" "<<h_date<<" "<<h_amount<<" "<<h_data<<endl;
	
	hist.h_c_id = Integer::castString(h_c_id, strlen(h_c_id));
	hist.h_c_d_id = Integer::castString(h_c_d_id, strlen(h_c_d_id));
	hist.h_c_w_id = Integer::castString(h_c_w_id, strlen(h_c_w_id));
	hist.h_d_id = Integer::castString(h_d_id, strlen(h_d_id));
	hist.h_w_id = Integer::castString(h_w_id, strlen(h_w_id));
	hist.h_date = Timestamp::castString(h_date, strlen(h_date));
    hist.h_amount = Numeric<6,2>::castString(h_amount, strlen(h_amount));
    hist.h_data = Varchar<24>::castString(h_data, strlen(h_data));
	t_history.push_back(hist);
  }
  
  fclose (pFile);
}

// Data parser for table neworder: loads data from tpcc_neworder.tbl file to t_neworder vector.
void load_neworder() {
  char no_o_id[512];
  char no_d_id[512];
  char no_w_id[512];
   
  char buff[1024];
  struct neworder no;
  FILE *pFile = fopen("tpcc_neworder.tbl", "r");
  uint64_t counter = 0;

  while(fgets(buff, 1024, pFile) != NULL) {
//    std::cout<<"line = "<<buff<<endl;
	
	sscanf(buff, "%[^|]|%[^|]|%s", no_o_id, no_d_id, no_w_id);
	
//    std::cout<<no_o_id<<" "<<no_d_id<<" "<<no_w_id<<endl;
	
	no.no_o_id = Integer::castString(no_o_id, strlen(no_o_id));
	no.no_d_id = Integer::castString(no_d_id, strlen(no_d_id));
	no.no_w_id = Integer::castString(no_w_id, strlen(no_w_id));
	t_neworder.push_back(no);
	
	map_neworder.insert(std::pair<tuple<Integer, Integer, Integer>, uint64_t>(std::tuple<Integer, Integer, Integer>(no.no_w_id, no.no_d_id, no.no_o_id), counter));
	counter += 1;
  }
  
  fclose (pFile);
}

// Data parser for table order: loads data from tpcc_order.tbl file to t_order vector.
void load_order() {
  char o_id[512];
  char o_d_id[512];
  char o_w_id[512];
  char o_c_id[512];
  char o_entry_d[512];
  char o_carrier_id[512];
  char o_ol_cnt[4];
  char o_all_local[3];
  
  char buff[1024];
  struct order odr;
  FILE *pFile = fopen("tpcc_order.tbl", "r");
  uint64_t counter = 0;
  
  while(fgets(buff, 1024, pFile) != NULL) {
//    std::cout<<"line = "<<buff<<endl;
	
	sscanf(buff, "%[^|]|%[^|]|%[^|]|%[^|]|%[^|]|%[^|]|%[^|]|%s", o_id, o_d_id, o_w_id, o_c_id, o_entry_d, o_carrier_id, o_ol_cnt, o_all_local);

//    std::cout<<o_id<<" "<<o_d_id<<" "<<o_w_id<<" "<<o_c_id<<" "<<o_entry_d<<" "<<o_carrier_id<<" "<<o_ol_cnt<<" "<<o_all_local<<endl;
	
	odr.o_id = Integer::castString(o_id, strlen(o_id));
	odr.o_d_id = Integer::castString(o_d_id, strlen(o_d_id));
	odr.o_w_id = Integer::castString(o_w_id, strlen(o_w_id));
	odr.o_c_id = Integer::castString(o_c_id, strlen(o_c_id));
	odr.o_entry_d = Timestamp::castString(o_entry_d, strlen(o_entry_d));
    odr.o_carrier_id = Integer::castString(o_carrier_id, strlen(o_carrier_id));
    odr.o_ol_cnt = Numeric<2,0>::castString(o_ol_cnt, strlen(o_ol_cnt));
    odr.o_all_local = Numeric<1,0>::castString(o_all_local, strlen(o_all_local));
	t_order.push_back(odr);
	
	map_order.insert(std::pair<tuple<Integer, Integer, Integer>, uint64_t>(std::tuple<Integer, Integer, Integer>(odr.o_w_id, odr.o_d_id,odr.o_id), counter));
	
	order_wdc.insert(std::pair<uint64_t, uint64_t>(hashKey<Integer, Integer, Integer, Integer>(odr.o_w_id, odr.o_d_id, odr.o_c_id, odr.o_id), counter));
	
//	order_wdc.insert(std::pair<tuple<Integer, Integer, Integer, Integer>, uint64_t>(std::tuple<Integer, Integer, Integer, Integer>(odr.o_w_id, odr.o_d_id, odr.o_c_id, odr.o_id), counter));

	counter += 1;
  }
  
  fclose (pFile);
}

// Data parser for table orderline: loads data from tpcc_orderline.tbl file to t_orderline vector.
void load_orderline() {
  char ol_o_id[512];
  char ol_d_id[512];
  char ol_w_id[512];
  char ol_number[512];
  char ol_i_id[512];
  char ol_supply_w_id[512];
  char ol_delivery_d[512];
  char ol_quantity[4];
  char ol_amount[10];
  char ol_dist_info[25];
  
  char buff[1024];
  struct orderline odr_l;
  FILE *pFile = fopen("tpcc_orderline.tbl", "r");
  uint64_t counter = 0;

  while(fgets(buff, 1024, pFile) != NULL) {
//    std::cout<<"line = "<<buff<<endl;
	
	sscanf(buff, "%[^|]|%[^|]|%[^|]|%[^|]|%[^|]|%[^|]|%[^|]|%[^|]|%[^|]|%s", ol_o_id, ol_d_id, ol_w_id, ol_number, ol_i_id, ol_supply_w_id, ol_delivery_d, ol_quantity, ol_amount, ol_dist_info);

//    std::cout<<ol_o_id<<" "<<ol_d_id<<" "<<ol_w_id<<" "<<ol_number<<" "<<ol_i_id<<" "<<ol_supply_w_id<<" "<<ol_delivery_d<<" "<<ol_quantity<<" "<<ol_amount<<" "<<ol_dist_info<<endl;
	
	odr_l.ol_o_id = Integer::castString(ol_o_id, strlen(ol_o_id));
	odr_l.ol_d_id = Integer::castString(ol_d_id, strlen(ol_d_id));
	odr_l.ol_w_id = Integer::castString(ol_w_id, strlen(ol_w_id));
	odr_l.ol_number = Integer::castString(ol_number, strlen(ol_number));
	odr_l.ol_i_id = Integer::castString(ol_i_id, strlen(ol_i_id));
	odr_l.ol_supply_w_id = Integer::castString(ol_supply_w_id, strlen(ol_supply_w_id));
	odr_l.ol_delivery_d = Timestamp::castString(ol_delivery_d, strlen(ol_delivery_d));
    odr_l.ol_quantity = Numeric<2,0>::castString(ol_quantity, strlen(ol_quantity));
    odr_l.ol_amount = Numeric<6,2>::castString(ol_amount, strlen(ol_amount));
    odr_l.ol_dist_info = Char<24>::castString(ol_dist_info, strlen(ol_dist_info));
	t_orderline.push_back(odr_l);
	
	map_orderline.insert(std::pair<tuple<Integer, Integer, Integer, Integer>, uint64_t>(std::tuple<Integer, Integer, Integer, Integer>(odr_l.ol_w_id, odr_l.ol_d_id, odr_l.ol_o_id, odr_l.ol_number), counter));
	counter += 1;
  }
  
  fclose (pFile);
}

// Data parser for table item: loads data from tpcc_item.tbl file to t_item vector.
void load_item() {
  char i_id[512];
  char i_im_id[512];
  char i_name[25];
  char i_price[9];
  char i_data[51];
  
  char buff[1024];
  struct item itm;
  FILE *pFile = fopen("tpcc_item.tbl", "r");
  uint64_t counter = 0;

  while(fgets(buff, 1024, pFile) != NULL) {
//    std::cout<<"line = "<<buff<<endl;
	
	sscanf(buff, "%[^|]|%[^|]|%[^|]|%[^|]|%s", i_id, i_im_id, i_name, i_price, i_data);

//    std::cout<<i_id<<" "<<i_im_id<<" "<<i_name<<" "<<i_price<<" "<<i_data<<endl;
	
	itm.i_id = Integer::castString(i_id, strlen(i_id));
	itm.i_im_id = Integer::castString(i_im_id, strlen(i_im_id));
	itm.i_name = Varchar<24>::castString(i_name, strlen(i_name));
    itm.i_price = Numeric<5,2>::castString(i_price, strlen(i_price));
    itm.i_data = Varchar<50>::castString(i_data, strlen(i_data));
	t_item.push_back(itm);
	
	map_item.insert(std::pair<Integer, uint64_t>(itm.i_id, counter));
	counter += 1;
  }
  
  fclose (pFile);
}

// Data parser for table stock: loads data from tpcc_stock.tbl file to t_stock vector.
void load_stock() {
  char s_i_id[512];
  char s_w_id[512];
  char s_quantity[6];
  char s_dist_01[25];
  char s_dist_02[25];
  char s_dist_03[25];
  char s_dist_04[25];
  char s_dist_05[25];
  char s_dist_06[25];
  char s_dist_07[25];
  char s_dist_08[25];
  char s_dist_09[25];
  char s_dist_10[25];
  char s_ytd[10];
  char s_order_cnt[6];
  char s_remote_cnt[6];
  char s_data[51];
  
  char buff[1024];
  struct stock stk;
  FILE *pFile = fopen("tpcc_stock.tbl", "r");
  uint64_t counter = 0;

  while(fgets(buff, 1024, pFile) != NULL) {
//    std::cout<<"line = "<<buff<<endl;
	
	sscanf(buff, "%[^|]|%[^|]|%[^|]|%[^|]|%[^|]|%[^|]|%[^|]|%[^|]|%[^|]|%[^|]|%[^|]|%[^|]|%[^|]|%[^|]|%[^|]|%[^|]|%s", s_i_id, s_w_id, s_quantity, s_dist_01, s_dist_02, s_dist_03, s_dist_04, s_dist_05, s_dist_06, s_dist_07, s_dist_08, s_dist_09, s_dist_10, s_ytd, s_order_cnt, s_remote_cnt, s_data);
	
//    std::cout<<s_i_id<<" "<<s_w_id<<" "<<s_quantity<<" "<<s_dist_01<<" "<<s_dist_02<<" "<<s_dist_03<<" "<<s_dist_04<<" "<<s_dist_05<<" "<<s_dist_06<<" "<<s_dist_07<<" "<<s_dist_08<<" "<<s_dist_09<<" "<<s_dist_10<<" "<<s_ytd<<" "<<s_order_cnt<<" "<<s_remote_cnt<<" "<<s_data<<endl;
	
	stk.s_i_id = Integer::castString(s_i_id, strlen(s_i_id));
	stk.s_w_id = Integer::castString(s_w_id, strlen(s_w_id));
    stk.s_quantity = Numeric<4,0>::castString(s_quantity, strlen(s_quantity));
    stk.s_dist_01 = Char<24>::castString(s_dist_01, strlen(s_dist_01));
    stk.s_dist_02 = Char<24>::castString(s_dist_02, strlen(s_dist_02));
    stk.s_dist_03 = Char<24>::castString(s_dist_03, strlen(s_dist_03));
    stk.s_dist_04 = Char<24>::castString(s_dist_04, strlen(s_dist_04));
    stk.s_dist_05 = Char<24>::castString(s_dist_05, strlen(s_dist_05));
    stk.s_dist_06 = Char<24>::castString(s_dist_06, strlen(s_dist_06));
    stk.s_dist_07 = Char<24>::castString(s_dist_07, strlen(s_dist_07));
    stk.s_dist_08 = Char<24>::castString(s_dist_08, strlen(s_dist_08));
    stk.s_dist_09 = Char<24>::castString(s_dist_09, strlen(s_dist_09));
    stk.s_dist_10 = Char<24>::castString(s_dist_10, strlen(s_dist_10));
    stk.s_ytd = Numeric<8,0>::castString(s_ytd, strlen(s_ytd));
    stk.s_order_cnt = Numeric<4,0>::castString(s_order_cnt, strlen(s_order_cnt));
    stk.s_remote_cnt = Numeric<4,0>::castString(s_remote_cnt, strlen(s_remote_cnt));
    stk.s_data = Varchar<50>::castString(s_data, strlen(s_data));
	t_stock.push_back(stk);
	
	map_stock.insert(std::pair<tuple<Integer, Integer>, uint64_t>(std::tuple<Integer, Integer>(stk.s_w_id, stk.s_i_id), counter));	
	counter += 1;
  }
  
  fclose (pFile);
}

void load() {
  load_warehouse();
  load_district();
  load_customer();
  load_history();
  load_neworder();
  load_order();
  load_orderline();
  load_item();
  load_stock();
  std::cout<<"All tables data loading done\n";
}

// To be called from neworderrandom.cpp
void newOrder(int32_t w_id, int32_t d_id, int32_t c_id, int32_t items, int32_t supware[], int32_t itemid[], int32_t qty[], Timestamp datetime) {
std::cout<<"neworder function called...\n";

  // For "select w_tax from warehouse w where w.w_id=w_id;"
  Numeric<4,4> w_tax_1 = t_warehouse[map_warehouse[(Integer)w_id]].w_tax;
std::cout<<"w_tax_1 = "<<w_tax_1<<"\n";

  // For "select c_discount from customer c where c_w_id=w_id and c_d_id=d_id and c.c_id=c_id;"  
  Numeric<4,4> c_discount_1 = t_customer[map_customer[std::tuple<Integer, Integer, Integer>((Integer)w_id, (Integer)d_id, (Integer)c_id)]].c_discount;
std::cout<<"c_discount_1 = "<<c_discount_1<<"\n";

  // For "select d_next_o_id as o_id,d_tax from district d where d_w_id=w_id and d.d_id=d_id;"
  Integer d_next_o_id_1 = t_district[map_district[std::tuple<Integer, Integer>((Integer)w_id, (Integer)d_id)]].d_next_o_id;
  Integer o_id = d_next_o_id_1;
  Numeric<4,4> d_tax = (Numeric<4,4>)d_next_o_id_1;
  std::cout<<"o_id="<<o_id<<" and d_tax= "<<d_tax<<endl;
  
  // For "update district set d_next_o_id=o_id+1 where d_w_id=w_id and district.d_id=d_id;"
  std::cout<<"old value od d_next_o_id = "<<t_district[map_district[std::tuple<Integer, Integer>((Integer)w_id, (Integer)d_id)]].d_next_o_id<<endl;
  t_district[map_district[std::tuple<Integer, Integer>((Integer)w_id, (Integer)d_id)]].d_next_o_id = o_id + 1;
  std::cout<<"new value od d_next_o_id = "<<t_district[map_district[std::tuple<Integer, Integer>((Integer)w_id, (Integer)d_id)]].d_next_o_id<<endl;

  int all_local = 1;
  for(int32_t index=0; index<=items-1; index++) {
    if(w_id==supware[index])
      all_local=0;
   }
   
return;

}


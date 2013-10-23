#include "Types.cpp"
#include "schema.cpp"
#include <iostream>
#include <cstring>
#include <cstdio>

using namespace std;

void load_warehouse() {
  char w_id[4];
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
    std::cout<<"line = "<<buff<<endl;  
	sscanf(buff, "%[^|]|%[^|]|%[^|]|%[^|]|%[^|]|%[^|]|%[^|]|%[^|]|%s", w_id, w_name, w_street_1, w_street_2, w_city, w_state, w_zip, w_tax, w_ytd);
    std::cout<<w_id<<" "<<w_name<<" "<<w_street_1<<" "<<w_street_2<<" "<<w_city<<" "<<w_state<<" "<<w_zip<<" "<<w_tax<<" "<<w_ytd<<endl;
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
	map_warehouse.insert(std::pair<Integer, uint64_t>(wh.w_id, (uint64_t)&t_warehouse[counter]));
	counter += 1;
  }
  
  fclose (pFile);
  }
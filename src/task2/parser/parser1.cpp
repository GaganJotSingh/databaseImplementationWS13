#include <iostream>
#include <memory>
#include <fstream>

#include "Parser.hpp"
#include "Schema.hpp"

using namespace std;

int main(int argc, char* argv[]) {
   if (argc != 2) {
      std::cerr << "usage: " << argv[0] << " <schema file>" << std::endl;
      return -1;
   }

   ofstream myfile ("parsedSchema.cpp");
   Parser p(argv[1]);
   if (myfile.is_open()) {
     try {
        std::unique_ptr<Schema> schema = p.parse();
        myfile << schema->toString();
     } catch (ParserError& e) {
        std::cerr << e.what() << std::endl;
     }
     myfile.close();
   } else {
     std::cout << "Unable to open file for writing"<<std::endl;
     return 0;
   }

   return 0;
}

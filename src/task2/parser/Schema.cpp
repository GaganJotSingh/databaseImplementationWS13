#include "Schema.hpp"

#include <sstream>

static std::string type(const Schema::Relation::Attribute& attr) {
   Types::Tag type = attr.type;
   switch(type) {
      case Types::Tag::Integer:
         return "Integer";
      case Types::Tag::Numeric: {
         std::stringstream ss;
         ss << "Numeric<" << attr.len1 << "," << attr.len2 << ">";
         return ss.str();
      }
      case Types::Tag::Char: {
         std::stringstream ss;
         ss << "Char<" << attr.len << ">";
         return ss.str();
      }
      case Types::Tag::Varchar: {
         std::stringstream ss;
         ss << "Varchar<" << attr.len << ">";
         return ss.str();
      }
      case Types::Tag::Timestamp:
         return "Timestamp";
   }
   throw;
}

std::string Schema::toString() const {
   std::stringstream out;
   out <<"#include \"Types.hpp\"\n#include <vector>\n#include <map>\n#include <tuple>\n\nusing namespace std;\n\n";
   for (const Schema::Relation& rel : relations) {
      out << "struct " << rel.name << " {" << std::endl;
      for (const auto& attr : rel.attributes)
         out << '\t' << type(attr) << " " << attr.name << ";" << std::endl;
      out << "};\n";
      if(rel.primaryKey.size() > 0) {
         out << "std::map<";
         if(rel.primaryKey.size() == 1) {
            out << type(rel.attributes[rel.primaryKey.back()]);
         } else {
            out << "tuple<";
            for (unsigned pKeyId : rel.primaryKey) {
               out << type(rel.attributes[pKeyId]);
               if(pKeyId != rel.primaryKey.back()) {
                  out << ", ";
               }
            }
            out << ">";
         }
         out << ", uint64_t> " << "map_" + rel.name << ";\n";
      }
      out << "vector<" << rel.name << "> " << "t_" + rel.name << ";\n\n";
   }
         
   return out.str();
}

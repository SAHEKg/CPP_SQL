#pragma once

#include "database.h"

namespace DB {

    class Controller {
    private:
        MyAwesomeDB *database_;

        std::regex reg_create = std::regex(R"(^\s*CREATE TABLE\s+([\w_]+)\s+\(([\S\s]+,)\s*PRIMARY\s*KEY\(.+\)\)\s*;)");
        std::regex reg_pairs_csv = std::regex(R"(^\s*([\w_]+)\s+([\w_]+)\s*,\s*([\S\s]*))");
        std::regex reg_csv = std::regex(R"(^\s*([\w_=\.]+)\s*,\s*(.*))");
        std::regex reg_single = std::regex(R"(^\s*([\w*_\s><=\"\.]+))");
        std::regex reg_drop = std::regex(R"(^\s*DROP TABLE\s+([\w_]+);)");
        std::regex reg_select_where_join = std::regex(
                R"(^\s*SELECT\s+([\w*,_\s\.]+)FROM\s+([\w_]+)\s+(INNER|LEFT|RIGHT)\s+JOIN\s+([\w_]+)\s+ON\s+([\S\s]+)WHERE\s+([\S\s]+);)");
        std::regex reg_select_join = std::regex(
                R"(^\s*SELECT\s+([\w*,_\s\.]+)FROM\s+([\w_]+)\s+(INNER|LEFT|RIGHT)\s+JOIN\s+([\w_]+)\s+ON\s+([\S\s]+);)");
        std::regex reg_select_where = std::regex(R"(^\s*SELECT\s+([\w*,_\s\.]+)FROM\s+([\w,_\s]+)WHERE\s+([\S\s]+);)");
        std::regex reg_select = std::regex(R"(^\s*SELECT\s+([\w*,_\s\.]+)FROM\s+([\w,_\s]+);)");
        std::regex reg_insert = std::regex(
                R"(^\s*INSERT INTO\s+([\w_]+)\s+(\([\w_,\s]+\))?\s*VALUES\s+\(([\w_\.,\s]+)\)\s*;)");
        std::regex reg_or_separate = std::regex(R"(^\s*([\w_\s><=!\"\.]+)OR\s+([\w_\s><=!\"\.]+))");
        std::regex reg_and_separate = std::regex(R"(^\s*([\w_\s><=!\"\.]+)AND\s+([\w_\s><=!\"\.]+))");
        std::regex reg_condition = std::regex(R"(^\s*([\w_\.]+)\s*([><=!]+)\s*([\w_\"\.]+)\s*)");
        std::regex reg_delete = std::regex(R"(^\s*DELETE FROM\s+([\w_]+)\s+WHERE\s+([\S\s]+);)");
        std::regex reg_update = std::regex(R"(^\s*UPDATE\s+([\w_]+)\s+SET(.+)\s+WHERE\s+([\S\s]+);)");
        std::regex reg_assignment = std::regex(R"(^\s*([\w_]+)\s*=\s*([\w_]+)\s*)");

        static void StripSpaces(std::string &str);

    public:
        Controller()
                : database_(nullptr) {}

        explicit Controller(MyAwesomeDB &database)
                : database_(&database) {}

        ~Controller() {
            database_ = nullptr;
        }

        std::vector <std::pair<std::string, std::string>> ParsePairsCSV(std::string data);

        std::vector <std::string> ParseSeparated(std::string data, const std::regex &separation);

        Condition MakeSingleCondition(const std::string &condition);

        std::vector <std::vector<Condition>> GetConditions(const std::string& input);

        std::vector <std::pair<std::string, std::string>> GetValuePairs(std::string input);

        void ReadInput(const std::string &input);
    };

}

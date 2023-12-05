#include "DB_controller.h"

using namespace DB;

void Controller::StripSpaces(std::string& str) {
    str.erase(remove_if(str.begin(), str.end(), isspace), str.end());
}

std::vector<std::pair<std::string, std::string>> Controller::ParsePairsCSV(std::string data) {
    std::smatch match;
    std::vector<std::pair<std::string, std::string>> result;
    while (std::regex_search(data, match, reg_pairs_csv)) {
        result.emplace_back(match[1], match[2]);
        data = match[3];
    }

    return result;
}

std::vector<std::string> Controller::ParseSeparated(std::string data, const std::regex& separation) {
    std::smatch match;
    std::vector<std::string> result;
    while (std::regex_search(data, match, separation)) {
        result.emplace_back(match[1]);
        data = match[2];
    }
    if (std::regex_search(data, match, reg_single)) {
        result.emplace_back(match[1]);
    }

    return result;
}

Condition Controller::MakeSingleCondition(const std::string& condition) {
    std::smatch match;
    if (std::regex_search(condition, match, reg_condition))
        return {match[2], match[1], match[3]};
}

std::vector<std::vector<Condition>> Controller::GetConditions(const std::string& input) {
    std::smatch match;
    auto OR_separated = ParseSeparated(input, reg_or_separate);
    std::vector<std::vector<Condition>> result(OR_separated.size());
    std::vector<std::string> AND_separated;
    for (int i = 0; i < OR_separated.size(); ++i) {
        result[i] = std::vector<Condition>();
        AND_separated = ParseSeparated(OR_separated[i], reg_and_separate);
        for (auto& condition : AND_separated) {
            result[i].emplace_back(MakeSingleCondition(condition));
        }
    }

    return result;
}

std::vector<std::pair<std::string, std::string>> Controller::GetValuePairs(std::string input) {
    std::smatch match;
    std::vector<std::pair<std::string, std::string>> result;
    StripSpaces(input);
    auto pairs = ParseSeparated(input, reg_csv);
    for (auto& elem : pairs) {
        if (std::regex_search(elem, match, reg_assignment)) {
            result.emplace_back(match[1], match[2]);
        }
    }

    return result;
}

void Controller::ReadInput(const std::string& input) {
    std::smatch match;
    if (std::regex_search(input, match, reg_create)) {
        std::string name = match[1];
        StripSpaces(name);
        std::string parameters = match[2];
        std::vector<std::pair<std::string, std::string>> columns = ParsePairsCSV(parameters);
        database_->CreateTable(name, columns);
        std::cout << std::endl << "-- TABLE " << name << " CREATED --\n" << std::endl;
    } else if (std::regex_search(input, match, reg_drop)) {
        std::string name = match[1];
        StripSpaces(name);
        database_->DeleteTable(name);
        std::cout << std::endl << "-- TABLE " << name << " DELETED --\n" << std::endl;
    } else if (std::regex_search(input, match, reg_select_where_join)) {
        std::vector<std::string> columns = ParseSeparated(match[1], reg_csv);
        std::string table_l = match[2];
        StripSpaces(table_l);
        std::string type = match[3];
        std::string table_r = match[4];
        StripSpaces(table_r);
        auto join_on = GetConditions(match[5]);
        auto conditions = GetConditions(match[6]);
        auto output = database_->SelectJoined(table_l, table_r, type, join_on, columns, conditions);
        for (auto& row : output) {
            std::cout << row << std::endl;
        }
    } else if (std::regex_search(input, match, reg_select_join)) {
        std::vector<std::string> columns = ParseSeparated(match[1], reg_csv);
        std::string table_l = match[2];
        StripSpaces(table_l);
        std::string type = match[3];
        std::string table_r = match[4];
        StripSpaces(table_r);
        auto join_on = GetConditions(match[5]);
        auto output = database_->SelectAllJoined(table_l, table_r, type, join_on, columns);
        for (auto& row : output) {
            std::cout << row << std::endl;
        }
    } else if (std::regex_search(input, match, reg_select_where)) {
        std::vector<std::string> columns = ParseSeparated(match[1], reg_csv);
        std::string table = match[2];
        StripSpaces(table);
        auto conditions = GetConditions(match[3]);
        auto output = database_->Select(table, columns, conditions);
        for (auto& row : output) {
            std::cout << row << std::endl;
        }
    } else if (std::regex_search(input, match, reg_select)) {
        std::vector<std::string> columns = ParseSeparated(match[1], reg_csv);
        std::string table = match[2];
        StripSpaces(table);
        auto output = database_->SelectAll(table, columns);
        for (auto& row : output) {
            std::cout << row << std::endl;
        }
    } else if (std::regex_search(input, match, reg_insert)) {
        std::string name = match[1];
        StripSpaces(name);
        std::vector<std::string> columns;
        if (match[2] == "")
            columns = {""};
        else
            columns = ParseSeparated(match[2].str().substr(1, match[2].str().size() - 2), reg_csv);
        std::vector<std::string> values = ParseSeparated(match[3], reg_csv);
        database_->Insert(name, columns, values);
    } else if (std::regex_search(input, match, reg_delete)) {
        std::string name = match[1];
        StripSpaces(name);
        auto conditions = GetConditions(match[2]);
        database_->Delete(name, conditions);
    } else if (std::regex_search(input, match, reg_update)) {
        std::string name = match[1];
        StripSpaces(name);
        auto values = GetValuePairs(match[2]);
        auto conditions = GetConditions(match[3]);
        database_-> Update(name, values, conditions);
    } else {
        std::cout << "-- INVALID COMMAND --\n" << std::endl;
    }
}

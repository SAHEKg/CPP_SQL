#include "database.h"

using namespace DB;

Types Column::type() const {
    return type_;
}

int Column::width() const {
    return width_;
}

void Column::SetWidth(int width) {
    width_ = width;
}

void Column::CheckWidth(int width) {
    width_ = std::max(width, width_);
}

const std::string& Condition::symbol() const {
    return symbol_;
}

const std::string& Condition::lhs() const {
    return lhs_;
}

const std::string& Condition::rhs() const {
    return rhs_;
}

void Row::Concatenate(const Row& lhs, const Row& rhs) {
    for (const auto& elem : lhs.data_) {
        Set(elem.first, elem.second);
    }
    for (const auto& elem : rhs.data_) {
        Set(elem.first, elem.second);
    }
}

std::string Row::Get(const std::string& name) {
    return data_[name];
}

void Row::Set(const std::string& name, const std::string& value) {
    if (data_.find(name) != data_.end())
        data_[name] = value;
}

bool Row::Suites(const std::map<std::string, Column>& columns) const {
    for (auto& elem : data_) {
        if (columns.find(elem.first) == columns.end())
            return false;
    }
    if (data_.size() == columns.size())
        return true;
    return false;
}

size_t Table::Size() {
    return rows_.size();
}

std::map<std::string, Column> Table::columns() {
    return columns_;
}

const Row& Table::GetRow(int index) {
    return rows_[index];
}

void Table::AddRow(const Row& row) {
    if (row.Suites(columns_))
        rows_.emplace_back(row);
}

void Table::Insert(std::vector<std::string> columns, const std::vector<std::string>& values) {
    Row new_row(columns_);
    if (columns[0].empty()) {
        columns.clear();
        for (auto column : columns_) {
            columns.emplace_back(column.first);
        }
    }
    for (int i = 0; i < columns.size(); ++i) {
        new_row.Set(columns[i], values[i]);
        columns_[columns[i]].CheckWidth(values[i].size());
    }
    rows_.emplace_back(new_row);

    std::cout << std::endl << "-- INSERTED " <<  values.size() << " VALUES --\n" << std::endl;
}

std::string Table::Get(int index, const std::string& name) {
    if (IsColumnName(name))
        return rows_[index].Get(name);
    else
        return name;
}

Types Table::GetType(const std::string& name) {
    return columns_[name].type();
}

bool Table::IsColumnName(const std::string& value) {
    for (auto& column : columns_) {
        if (column.first == value)
            return true;
    }

    return false;
}

void Table::Delete(const std::vector<bool>& selected) {
    int counter = 0;
    for (int i = 0; i < selected.size(); ++i) {
        if (selected[i]) {
            rows_.erase(rows_.begin() + i);
            ++counter;
        }
    }
    std::cout << std::endl << "-- DELETED " << counter << " ROWS --\n" << std::endl;
}

void Table::UpdateWidth(const std::string& name) {
    columns_[name].SetWidth(0);
    for (auto& row : rows_) {
        columns_[name].CheckWidth(row.Get(name).size());
    }
}

void Table::Update(const std::vector<std::pair<std::string, std::string>>& values, const std::vector<bool>& selected) {
    int counter = 0;
    for (int i = 0; i < selected.size(); ++i) {
        if (selected[i]) {
            for (auto& pair : values) {
                rows_[i].Set(pair.first, pair.second);
            }
            ++counter;
        }
    }
    for (auto& pair : values) {
        UpdateWidth(pair.first);
    }
    std::cout << std::endl << "-- UPDATED " << counter << " ROWS --\n" << std::endl;
}

void MyAwesomeDB::StripSpaces(std::string& str) {
    str.erase(remove_if(str.begin(), str.end(), isspace), str.end());
}

std::string MyAwesomeDB::GetColumnName(const std::string& value) {
    std::smatch match;
    if (std::regex_search(value, match, reg_dot_separated))
        return match[2];
    return value;
}

std::string MyAwesomeDB::MakeDivider(const std::string& table, const std::vector<std::string>& columns) {
    std::string result;
    int width;
    result = "+";
    for (auto& name : columns) {
        width = tables_[table]->columns()[GetColumnName(name)].width();
        for (int i = 0; i < width + 2; ++i) {
            result += "-";
        }
        result += "+";
    }

    return result;
}

std::vector<std::string> MyAwesomeDB::MakeOutput(const std::string& table, std::vector<std::string> columns,
                                                 const std::vector<bool>& selected, bool select_all = false) {
    for (auto& elem : columns) {
        StripSpaces(elem);
    }
    if (columns[0] == "*" && columns.size() == 1) {
        columns.clear();
        for (auto& column : tables_[table]->columns()) {
            columns.emplace_back(column.first);
        }
    }
    std::vector<std::string> output;
    std::string row = "| ";
    int width;
    std::string divider = MakeDivider(table, columns);
    output.emplace_back(divider);
    for (auto& name : columns) {
        if (tables_[table]->columns().find(GetColumnName(name)) == tables_[table]->columns().end())
            continue;
        width = tables_[table]->columns()[GetColumnName(name)].width();
        row += GetColumnName(name);
        for (int i = 0; i < width - GetColumnName(name).size(); ++i) {
            row += " ";
        }
        row += " | ";
    }
    output.emplace_back(row);
    output.emplace_back(divider);
    for (int i = 0; i < tables_[table]->Size(); ++i) {
        if (select_all || selected[i]) {
            row = "| ";
            for (auto& name : columns) {
                if (tables_[table]->columns().find(GetColumnName(name)) == tables_[table]->columns().end())
                    continue;
                width = tables_[table]->columns()[GetColumnName(name)].width();
                row += GetValue({{table, i}, {table, i}}, GetColumnName(name));
                for (int j = 0; j < width - GetValue({{table, i}, {table, i}}, GetColumnName(name)).size(); ++j) {
                    row += " ";
                }
                row += " | ";
            }
            output.emplace_back(row);
        }
    }
    output.emplace_back(divider + "\n");

    return output;
}

Types MyAwesomeDB::SeeType(const std::string& str) {
    if (str == "INT")
        return INT;
    else if (str == "BOOL")
        return BOOL;
    else if (str == "DOUBLE")
        return DOUBLE;
    else if (str == "TEXT")
        return TEXT;
    return UNKNOWN;
}

void MyAwesomeDB::CreateTable(const std::string& name, const std::vector<std::pair<std::string, std::string>>& columns) {
    std::map<std::string, Column> columns_;
    Types type;
    for (auto& column : columns) {
        type = SeeType(column.second);
        columns_.insert({column.first, Column(type, column.first.size())});
    }

    tables_.insert({name, new Table(columns_)});
}

void MyAwesomeDB::DeleteTable(const std::string& name) {
    tables_.erase(name);
}

std::vector<std::string> MyAwesomeDB::SelectAll(const std::string& table, const std::vector<std::string>& columns) {
    if (tables_.find(table) == tables_.end())
        return {"-- NO TABLE " + table + " FOUND --\n"};
    std::vector<bool> empty;

    return MakeOutput(table, columns, empty, true);
}

bool MyAwesomeDB::Check(int lhs, int rhs, const std::string& symbol) {
    if (symbol == ">") {
        return lhs > rhs;
    } else if (symbol == ">=") {
        return lhs >= rhs;
    } else if (symbol == "<") {
        return lhs < rhs;
    } else if (symbol == "<=") {
        return lhs <= rhs;
    } else if (symbol == "=") {
        return lhs == rhs;
    } else if (symbol == "!=") {
        return lhs != rhs;
    }
    return false;
}

bool MyAwesomeDB::Check(bool lhs, bool rhs, const std::string& symbol) {
    if (symbol == ">") {
        return lhs > rhs;
    } else if (symbol == ">=") {
        return lhs >= rhs;
    } else if (symbol == "<") {
        return lhs < rhs;
    } else if (symbol == "<=") {
        return lhs <= rhs;
    } else if (symbol == "=") {
        return lhs == rhs;
    } else if (symbol == "!=") {
        return lhs != rhs;
    }
    return false;
}

bool MyAwesomeDB::Check(double lhs, double rhs, const std::string& symbol) {
    if (symbol == ">") {
        return lhs > rhs;
    } else if (symbol == ">=") {
        return lhs >= rhs;
    } else if (symbol == "<") {
        return lhs < rhs;
    } else if (symbol == "<=") {
        return lhs <= rhs;
    } else if (symbol == "=") {
        return lhs == rhs;
    } else if (symbol == "!=") {
        return lhs != rhs;
    }
    return false;
}

bool MyAwesomeDB::Check(const std::string& lhs, const std::string& rhs, const std::string& symbol) {
    if (symbol == ">") {
        return lhs > rhs;
    } else if (symbol == ">=") {
        return lhs >= rhs;
    } else if (symbol == "<") {
        return lhs < rhs;
    } else if (symbol == "<=") {
        return lhs <= rhs;
    } else if (symbol == "=") {
        return lhs == rhs;
    } else if (symbol == "!=") {
        return lhs != rhs;
    }
    return false;
}

std::string MyAwesomeDB::GetValue(std::map<std::string, int> indexes, std::string name) {
    std::smatch match;
    std::string table = indexes.begin()->first;
    if (std::regex_search(name, match, reg_dot_separated)) {
        table = match[1];
        name = match[2];
    }

    return tables_[table]->Get(indexes[table], name);
}

Types MyAwesomeDB::GetType(std::string table, std::string name) {
    std::smatch match;
    if (std::regex_search(name, match, reg_dot_separated)) {
        table = match[1];
        name = match[2];
    }

    return tables_[table]->GetType(name);
}

bool MyAwesomeDB::CheckCondition(const std::map<std::string, int>& indexes, const std::string& lhs,
                                 const std::string& rhs, const std::string& symbol) {
    Types type = GetType(indexes.begin()->first, lhs);
    std::string lhs_value = GetValue(indexes, lhs);
    std::string rhs_value = GetValue(indexes, rhs);
    if (type == INT) {
        return Check(std::stoi(lhs_value), std::stoi(rhs_value), symbol);
    } else if (type == BOOL) {
        return Check(lhs_value == "1", rhs_value == "1", symbol);
    } else if (type == DOUBLE) {
        return Check(std::stof(lhs_value), std::stof(rhs_value), symbol);
    } else if (type == TEXT) {
        return Check(lhs_value, rhs_value, symbol);
    }
}

bool MyAwesomeDB::CheckRow(const std::map<std::string, int>& indexes,
                           const std::vector<std::vector<Condition>>& conditions) {
    bool result = false;
    bool AND_result;
    bool tmp;
    for (auto& AND_separated : conditions) {
        AND_result = CheckCondition(indexes, AND_separated[0].lhs(), AND_separated[0].rhs(),
                                    AND_separated[0].symbol());
        for (int i = 1; i < AND_separated.size(); ++i) {
            tmp = CheckCondition(indexes, AND_separated[i].lhs(), AND_separated[i].rhs(),
                                 AND_separated[i].symbol());
            AND_result = AND_result && tmp;
        }
        result = result || AND_result;
    }

    return result;
}

std::vector<bool> MyAwesomeDB::GetRows(const std::string& table, const std::vector<std::vector<Condition>>& conditions) {
    std::vector<bool> result(tables_[table]->Size(), false);
    for (int i = 0; i < result.size(); ++i) {
        result[i] = CheckRow({{table, i}, {table, i}}, conditions);
    }

    return result;
}

std::vector<std::string> MyAwesomeDB::Select(const std::string& table, const std::vector<std::string>& columns,
                                const std::vector<std::vector<Condition>>& conditions) {
    if (tables_.find(table) == tables_.end())
        return {"-- NO TABLE " + table + " FOUND --\n"};
    auto selected = GetRows(table, conditions);

    return MakeOutput(table, columns, selected);
}

void MyAwesomeDB::Insert(const std::string& table, const std::vector<std::string>& columns, const std::vector<std::string>& values) {
    if (tables_.find(table) == tables_.end()) {
        std::cout << "-- NO TABLE " + table + " FOUND --\n" << std::endl;
        return;
    }
    tables_[table]->Insert(columns, values);
}

void MyAwesomeDB::Delete(const std::string& table, const std::vector<std::vector<Condition>>& conditions) {
    if (tables_.find(table) == tables_.end()) {
        std::cout << "-- NO TABLE " + table + " FOUND --\n" << std::endl;
        return;
    }
    auto selected = GetRows(table, conditions);
    tables_[table]->Delete(selected);
}

void MyAwesomeDB::Update(const std::string& table, const std::vector<std::pair<std::string, std::string>>& values,
            const std::vector<std::vector<Condition>>& conditions) {
    if (tables_.find(table) == tables_.end()) {
        std::cout << "-- NO TABLE " + table + " FOUND --\n" << std::endl;
        return;
    }
    auto selected = GetRows(table, conditions);
    tables_[table]->Update(values, selected);
}

Table* MyAwesomeDB::InnerJoin(const std::string& table_l, const std::string& table_r, const std::vector<std::vector<Condition>>& join_on) {
    int size_l = tables_[table_l]->Size();
    int size_r = tables_[table_r]->Size();
    std::map<std::string, Column> new_columns;
    new_columns = tables_[table_l]->columns();
    for (auto& column : tables_[table_r]->columns()) {
        if (new_columns.find(column.first) == new_columns.end())
            new_columns.insert(column);
    }
    auto new_table = new Table(new_columns);
    for (int l = 0; l < size_l; ++l) {
        for (int r = 0; r < size_r; ++r) {
            if (CheckRow({{table_l, l}, {table_r, r}}, join_on)) {
                auto new_row = Row(new_columns);
                new_row.Concatenate(tables_[table_l]->GetRow(l), tables_[table_r]->GetRow(r));
                new_table->AddRow(new_row);
            }
        }
    }

    return new_table;
}

Table* MyAwesomeDB::LeftJoin(const std::string& table_l, const std::string& table_r, const std::vector<std::vector<Condition>>& join_on) {
    int size_l = tables_[table_l]->Size();
    int size_r = tables_[table_r]->Size();
    std::map<std::string, Column> new_columns;
    new_columns = tables_[table_l]->columns();
    for (auto& column : tables_[table_r]->columns()) {
        if (new_columns.find(column.first) == new_columns.end())
            new_columns.insert(column);
    }
    auto new_table = new Table(new_columns);
    Row new_row;
    bool found;
    for (int l = 0; l < size_l; ++l) {
        found = false;
        for (int r = 0; r < size_r; ++r) {
            if (CheckRow({{table_l, l}, {table_r, r}}, join_on)) {
                new_row = Row(new_columns);
                new_row.Concatenate(tables_[table_l]->GetRow(l), tables_[table_r]->GetRow(r));
                new_table->AddRow(new_row);
                found = true;
            }
        }
        if (!found) {
            new_row = Row(new_columns);
            new_row.Concatenate(Row(tables_[table_r]->columns()), tables_[table_l]->GetRow(l));
            new_table->AddRow(new_row);
        }
    }

    return new_table;
}

Table* MyAwesomeDB::RightJoin(const std::string& table_l, const std::string& table_r, const std::vector<std::vector<Condition>>& join_on) {
    return LeftJoin(table_r, table_l, join_on);
}

std::vector<std::string> MyAwesomeDB::SelectAllJoined(const std::string& table_l, const std::string& table_r,
                                         const std::string& join_type, const std::vector<std::vector<Condition>>& join_on,
                                         const std::vector<std::string>& columns) {
    if (tables_.find(table_l) == tables_.end())
        return {"-- NO TABLE " + table_l + " FOUND --\n"};
    if (tables_.find(table_r) == tables_.end())
        return {"-- NO TABLE " + table_r + " FOUND --\n"};
    Table* joined;
    if (join_type == "INNER") {
        joined = InnerJoin(table_l, table_r, join_on);
    } else if (join_type == "LEFT") {
        joined = LeftJoin(table_l, table_r, join_on);
    } else if (join_type == "RIGHT") {
        joined = RightJoin(table_l, table_r, join_on);
    }
    std::string name = table_l+"join"+table_r;
    tables_.insert({name, joined});
    std::vector<bool> empty;
    std::vector<std::string> result = MakeOutput(name, columns, empty, true);
    tables_.erase(name);
    delete joined;

    return result;
}

std::vector<std::string> MyAwesomeDB::SelectJoined(const std::string& table_l, const std::string& table_r,
                                      const std::string& join_type, const std::vector<std::vector<Condition>>& join_on,
                                      const std::vector<std::string>& columns,
                                      const std::vector<std::vector<Condition>>& conditions) {
    if (tables_.find(table_l) == tables_.end())
        return {"-- NO TABLE " + table_l + " FOUND --\n"};
    if (tables_.find(table_r) == tables_.end())
        return {"-- NO TABLE " + table_r + " FOUND --\n"};
    Table* joined;
    if (join_type == "INNER") {
        joined = InnerJoin(table_l, table_r, join_on);
    } else if (join_type == "LEFT") {
        joined = LeftJoin(table_l, table_r, join_on);
    } else if (join_type == "RIGHT") {
        joined = RightJoin(table_l, table_r, join_on);
    }
    std::string name = table_l+"join"+table_r;
    tables_.insert({name, joined});
    auto selected = GetRows(name, conditions);
    std::vector<std::string> result = MakeOutput(name, columns, selected);
    tables_.erase(name);
    delete joined;

    return result;
}

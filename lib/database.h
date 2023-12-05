#pragma once

#include <iostream>
#include <vector>
#include <map>
#include <variant>
#include <regex>

namespace DB {

    enum Types {
        BOOL,
        INT,
        DOUBLE,
        TEXT,
        UNKNOWN
    };

    class Column {
    private:
        Types type_;
        int width_;

    public:
        Column() = default;

        Column(Types type, int width)
                : type_(type)
                , width_(width)
        {}

        Types type() const;

        int width() const;

        void SetWidth(int width);

        void CheckWidth(int width);
    };

    class Condition {
    private:
        std::string symbol_;
        std::string lhs_;
        std::string rhs_;

    public:
        Condition() = default;

        Condition(const std::string& symbol, const std::string& lhs, const std::string& rhs)
                : symbol_(symbol)
                , lhs_(lhs)
                , rhs_(rhs)
        {}

        const std::string& symbol() const;

        const std::string& lhs() const;

        const std::string& rhs() const;
    };

    class Row {
    private:
        std::map<std::string, std::string> data_;

    public:
        Row() = default;

        explicit Row(const std::map<std::string, Column>& columns) {
            for (auto& column : columns) {
                data_.insert({column.first, ""});
            }
        }

        void Concatenate(const Row& lhs, const Row& rhs);

        std::string Get(const std::string& name);

        void Set(const std::string& name, const std::string& value);

        bool Suites(const std::map<std::string, Column>& columns) const;
    };

    class Table {
    private:
        std::vector<Row> rows_;
        std::map<std::string, Column> columns_;

    public:
        Table() = default;

        explicit Table(const std::map<std::string, Column>& columns)
                : columns_(columns)
        {}

        size_t Size();

        std::map<std::string, Column> columns();

        const Row& GetRow(int index);

        void AddRow(const Row& row);

        void Insert(std::vector<std::string> columns, const std::vector<std::string>& values);

        std::string Get(int index, const std::string& name);

        Types GetType(const std::string& name);

        bool IsColumnName(const std::string& value);

        void Delete(const std::vector<bool>& selected);

        void UpdateWidth(const std::string& name);

        void Update(const std::vector<std::pair<std::string, std::string>>& values, const std::vector<bool>& selected);
    };

    class MyAwesomeDB {
    private:
        std::map<std::string, Table*> tables_;

        static void StripSpaces(std::string& str);

        std::string GetColumnName(const std::string& value);

        std::regex reg_dot_separated = std::regex(R"(\s*([\w_]+)\.([\w_]+)\s*)");

    public:
        MyAwesomeDB() = default;

        ~MyAwesomeDB() {
            for (auto& table : tables_) {
                delete table.second;
            }
        }

        std::string MakeDivider(const std::string& table, const std::vector<std::string>& columns);

        std::vector<std::string> MakeOutput(const std::string& table, std::vector<std::string> columns,
                                            const std::vector<bool>& selected, bool select_all);

        static Types SeeType(const std::string& str);

        void CreateTable(const std::string& name, const std::vector<std::pair<std::string, std::string>>& columns);

        void DeleteTable(const std::string& name);

        std::vector<std::string> SelectAll(const std::string& table, const std::vector<std::string>& columns);

        static bool Check(int lhs, int rhs, const std::string& symbol);

        static bool Check(bool lhs, bool rhs, const std::string& symbol);

        static bool Check(double lhs, double rhs, const std::string& symbol);

        static bool Check(const std::string& lhs, const std::string& rhs, const std::string& symbol);

        std::string GetValue(std::map<std::string, int> indexes, std::string name);

        Types GetType(std::string table, std::string name);

        bool CheckCondition(const std::map<std::string, int>& indexes, const std::string& lhs,
                            const std::string& rhs, const std::string& symbol);

        bool CheckRow(const std::map<std::string, int>& indexes,
                      const std::vector<std::vector<Condition>>& conditions);

        std::vector<bool> GetRows(const std::string& table, const std::vector<std::vector<Condition>>& conditions);

        std::vector<std::string> Select(const std::string& table, const std::vector<std::string>& columns,
                                        const std::vector<std::vector<Condition>>& conditions);

        void Insert(const std::string& table, const std::vector<std::string>& columns, const std::vector<std::string>& values);

        void Delete(const std::string& table, const std::vector<std::vector<Condition>>& conditions);

        void Update(const std::string& table, const std::vector<std::pair<std::string, std::string>>& values,
                    const std::vector<std::vector<Condition>>& conditions);

        Table* InnerJoin(const std::string& table_l, const std::string& table_r, const std::vector<std::vector<Condition>>& join_on);

        Table* LeftJoin(const std::string& table_l, const std::string& table_r, const std::vector<std::vector<Condition>>& join_on);

        Table* RightJoin(const std::string& table_l, const std::string& table_r, const std::vector<std::vector<Condition>>& join_on);

        std::vector<std::string> SelectAllJoined(const std::string& table_l, const std::string& table_r,
                                                 const std::string& join_type, const std::vector<std::vector<Condition>>& join_on,
                                                 const std::vector<std::string>& columns);

        std::vector<std::string> SelectJoined(const std::string& table_l, const std::string& table_r,
                                              const std::string& join_type, const std::vector<std::vector<Condition>>& join_on,
                                              const std::vector<std::string>& columns,
                                              const std::vector<std::vector<Condition>>& conditions);
    };

}

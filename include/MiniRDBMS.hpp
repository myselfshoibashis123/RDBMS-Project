#pragma once
#include <iostream>
#include <vector>
#include <string>
#include <variant>

enum class DataType { INT, STRING };
using Value = std::variant<int, std::string>;

struct Attribute {
    std::string name;
    DataType type;
    Attribute(std::string name, DataType type);
};

class Record {
public:
    std::vector<Value> values;
    Record(std::vector<Value> vals);
};

class Table {
public:
    std::string name;
    std::vector<Attribute> schema;
    std::vector<Record> records;

    Table(std::string tableName, std::vector<Attribute> attrs);
    void insert(Record r);
    void print() const;
    std::string getName() const;

    void selectWhere(const std::string& attrName, const std::string& op, const Value& val);
    void selectColumns(const std::vector<std::string>& columns);
    void deleteWhere(const std::string& attrName, const std::string& op, const Value& val);
    void updateWhere(const std::string& attrName, const std::string& op, const Value& val,
                     const std::string& updateAttr, const Value& newVal);

    Table intersect(const Table& other) const;
    Table innerJoin(const Table& other, const std::string& thisKey, const std::string& otherKey) const;
    Table setUnion(const Table& other) const;
};

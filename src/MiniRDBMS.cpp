#include "../include/MiniRDBMS.hpp"
#include <iomanip>
#include <set>
#include <map>
#include <algorithm>

using namespace std;

Attribute::Attribute(string name, DataType type) : name(name), type(type) {}
Record::Record(vector<Value> vals) : values(vals) {}
Table::Table(string tableName, vector<Attribute> attrs) : name(tableName), schema(attrs) {}

void Table::insert(Record r) {
    records.push_back(r);
}

void Table::print() const {
    cout << "Table: " << name << "\n";
    for (const auto& attr : schema)
        cout << setw(10) << attr.name;
    cout << "\n";
    for (const auto& rec : records) {
        for (const auto& val : rec.values) {
            if (holds_alternative<int>(val))
                cout << setw(10) << get<int>(val);
            else
                cout << setw(10) << get<string>(val);
        }
        cout << "\n";
    }
}

string Table::getName() const { return name; }

void Table::selectWhere(const string& attrName, const string& op, const Value& val) {
    int index = -1;
    for (int i = 0; i < schema.size(); ++i)
        if (schema[i].name == attrName)
            index = i;

    for (const auto& rec : records) {
        bool condition = false;
        if (holds_alternative<int>(val) && holds_alternative<int>(rec.values[index])) {
            int v1 = get<int>(val);
            int v2 = get<int>(rec.values[index]);
            if (op == "=") condition = v2 == v1;
            else if (op == "<") condition = v2 < v1;
            else if (op == ">") condition = v2 > v1;
        } else if (holds_alternative<string>(val) && holds_alternative<string>(rec.values[index])) {
            string v1 = get<string>(val);
            string v2 = get<string>(rec.values[index]);
            if (op == "=") condition = v2 == v1;
        }
        if (condition) {
            for (const auto& v : rec.values) {
                if (holds_alternative<int>(v)) cout << setw(10) << get<int>(v);
                else cout << setw(10) << get<string>(v);
            }
            cout << "\n";
        }
    }
}

void Table::deleteWhere(const string& attrName, const string& op, const Value& val) {
    int index = -1;
    for (int i = 0; i < schema.size(); ++i)
        if (schema[i].name == attrName)
            index = i;

    records.erase(remove_if(records.begin(), records.end(), [&](const Record& rec) {
        bool condition = false;
        if (holds_alternative<int>(val) && holds_alternative<int>(rec.values[index])) {
            int v1 = get<int>(val);
            int v2 = get<int>(rec.values[index]);
            if (op == "=") condition = v2 == v1;
            else if (op == "<") condition = v2 < v1;
            else if (op == ">") condition = v2 > v1;
        } else if (holds_alternative<string>(val) && holds_alternative<string>(rec.values[index])) {
            string v1 = get<string>(val);
            string v2 = get<string>(rec.values[index]);
            if (op == "=") condition = v2 == v1;
        }
        return condition;
    }), records.end());
}

void Table::updateWhere(const string& attrName, const string& op, const Value& val,
                        const string& updateAttr, const Value& newVal) {
    int condIndex = -1, updateIndex = -1;
    for (int i = 0; i < schema.size(); ++i) {
        if (schema[i].name == attrName) condIndex = i;
        if (schema[i].name == updateAttr) updateIndex = i;
    }

    for (auto& rec : records) {
        bool condition = false;
        if (holds_alternative<int>(val) && holds_alternative<int>(rec.values[condIndex])) {
            int v1 = get<int>(val);
            int v2 = get<int>(rec.values[condIndex]);
            if (op == "=") condition = v2 == v1;
            else if (op == "<") condition = v2 < v1;
            else if (op == ">") condition = v2 > v1;
        } else if (holds_alternative<string>(val) && holds_alternative<string>(rec.values[condIndex])) {
            string v1 = get<string>(val);
            string v2 = get<string>(rec.values[condIndex]);
            if (op == "=") condition = v2 == v1;
        }
        if (condition)
            rec.values[updateIndex] = newVal;
    }
}

Table Table::intersect(const Table& other) const {
    Table result("intersect_result", schema);
    set<vector<Value>> s1, s2;
    for (const auto& rec : records) s1.insert(rec.values);
    for (const auto& rec : other.records) s2.insert(rec.values);
    for (const auto& row : s1)
        if (s2.count(row)) result.insert(Record(row));
    return result;
}

Table Table::setUnion(const Table& other) const {
    Table result("union_result", schema);
    set<vector<Value>> s;
    for (const auto& rec : records) s.insert(rec.values);
    for (const auto& rec : other.records) s.insert(rec.values);
    for (const auto& row : s) result.insert(Record(row));
    return result;
}

Table Table::innerJoin(const Table& other, const string& thisKey, const string& otherKey) const {
    int index1 = -1, index2 = -1;
    for (int i = 0; i < schema.size(); ++i)
        if (schema[i].name == thisKey) index1 = i;
    for (int j = 0; j < other.schema.size(); ++j)
        if (other.schema[j].name == otherKey) index2 = j;

    vector<Attribute> newSchema = schema;
    for (const auto& attr : other.schema) newSchema.push_back(attr);

    Table result("join_result", newSchema);
    for (const auto& rec1 : records) {
        for (const auto& rec2 : other.records) {
            if (rec1.values[index1] == rec2.values[index2]) {
                vector<Value> newRow = rec1.values;
                newRow.insert(newRow.end(), rec2.values.begin(), rec2.values.end());
                result.insert(Record(newRow));
            }
        }
    }
    return result;
}

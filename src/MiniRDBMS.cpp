#include "../include/MiniRDBMS.hpp"
#include <iostream>
#include <fstream>
#include <algorithm>
using namespace std;



Table::Table(std::string name, std::vector<Attribute> schema)
    : name(name), schema(schema) {}

void Table::insert(Record r) {
    records.push_back(r);
}

void Table::print() const {
    for (const auto& attr : schema) {
        cout << attr.name << "\t";
    }
    cout << endl;

    for (const auto& rec : records) {
        for (const auto& val : rec.values) {
            if (holds_alternative<int>(val))
                cout << get<int>(val) << "\t";
            else
                cout << get<string>(val) << "\t";
        }
        cout << endl;
    }
}

void Table::selectWhere(string col, string op, Value val) const {
    int idx = -1;
    for (int i = 0; i < schema.size(); ++i) {
        if (schema[i].name == col) {
            idx = i;
            break;
        }
    }

    if (idx == -1) {
        cout << "Column not found.\n";
        return;
    }

    for (const auto& rec : records) {
        const auto& v = rec.values[idx];
        bool match = false;
        if (op == "==") match = (v == val);
        else if (op == "!=") match = (v != val);
        else if (op == "<") match = (v < val);
        else if (op == ">") match = (v > val);
        else if (op == "<=") match = (v <= val);
        else if (op == ">=") match = (v >= val);

        if (match) {
            for (const auto& val : rec.values) {
                if (holds_alternative<int>(val))
                    cout << get<int>(val) << "\t";
                else
                    cout << get<string>(val) << "\t";
            }
            cout << endl;
        }
    }
}

void Table::deleteWhere(string col, string op, Value val) {
    int idx = -1;
    for (int i = 0; i < schema.size(); ++i) {
        if (schema[i].name == col) {
            idx = i;
            break;
        }
    }

    if (idx == -1) {
        cout << "Column not found.\n";
        return;
    }

    auto it = remove_if(records.begin(), records.end(), [&](const Record& rec) {
        const auto& v = rec.values[idx];
        if (op == "==") return v == val;
        if (op == "!=") return v != val;
        if (op == "<")  return v < val;
        if (op == ">")  return v > val;
        if (op == "<=") return v <= val;
        if (op == ">=") return v >= val;
        return false;
    });

    records.erase(it, records.end());
}

void Table::updateWhere(string colWhere, string op, Value val, string colSet, Value newVal) {
    int idxWhere = -1, idxSet = -1;

    for (int i = 0; i < schema.size(); ++i) {
        if (schema[i].name == colWhere) idxWhere = i;
        if (schema[i].name == colSet) idxSet = i;
    }

    if (idxWhere == -1 || idxSet == -1) {
        cout << "Column not found.\n";
        return;
    }

    for (auto& rec : records) {
        auto& targetVal = rec.values[idxWhere];
        bool match = false;

        if (op == "==") match = (targetVal == val);
        else if (op == "!=") match = (targetVal != val);
        else if (op == "<") match = (targetVal < val);
        else if (op == ">") match = (targetVal > val);
        else if (op == "<=") match = (targetVal <= val);
        else if (op == ">=") match = (targetVal >= val);

        if (match) {
            rec.values[idxSet] = newVal;
        }
    }
}

Table Table::intersect(const Table& other) const {
    Table result("IntersectResult", schema);

    for (const auto& rec : records) {
        for (const auto& orec : other.records) {
            if (rec.values == orec.values) {
                result.insert(rec);
                break;
            }
        }
    }

    return result;
}

Table Table::setUnion(const Table& other) const {
    Table result("UnionResult", schema);

    for (const auto& rec : records)
        result.insert(rec);

    for (const auto& orec : other.records) {
        bool exists = false;
        for (const auto& rec : result.records) {
            if (rec.values == orec.values) {
                exists = true;
                break;
            }
        }
        if (!exists) {
            result.insert(orec);
        }
    }

    return result;
}

Table Table::innerJoin(const Table& other, const string& col1, const string& col2) const {
    int idx1 = -1, idx2 = -1;

    for (int i = 0; i < schema.size(); ++i) {
        if (schema[i].name == col1) idx1 = i;
    }
    for (int i = 0; i < other.schema.size(); ++i) {
        if (other.schema[i].name == col2) idx2 = i;
    }

    if (idx1 == -1 || idx2 == -1) {
        cout << "Join columns not found.\n";
        return Table("InvalidJoin", {});
    }

    vector<Attribute> newSchema = schema;
    newSchema.insert(newSchema.end(), other.schema.begin(), other.schema.end());
    Table result("JoinResult", newSchema);

    for (const auto& rec1 : records) {
        for (const auto& rec2 : other.records) {
            if (rec1.values[idx1] == rec2.values[idx2]) {
                vector<Value> newRow = rec1.values;
                newRow.insert(newRow.end(), rec2.values.begin(), rec2.values.end());
                result.insert(Record(newRow));
            }
        }
    }

    return result;
}

void Table::saveToFile(const std::string& filename) const {
    ofstream out(filename);
    for (const auto& attr : schema) {
        out << attr.name << " " << (attr.type == DataType::INT ? "INT" : "STRING") << " ";
    }
    out << "#" << endl; // schema-data delimiter

    for (const auto& rec : records) {
        for (const auto& val : rec.values) {
            if (holds_alternative<int>(val))
                out << get<int>(val) << " ";
            else
                out << get<string>(val) << " ";
        }
        out << endl;
    }
}

Table Table::loadFromFile(const std::string& filename, const std::string& tablename) {
    ifstream in(filename);
    string word;
    vector<Attribute> schema;

    while (in >> word && word != "#") {
        string type;
        in >> type;
        schema.emplace_back(word, type == "INT" ? DataType::INT : DataType::STRING);
    }

    Table table(tablename, schema);
    vector<Value> row;
    while (in >> word) {
        if (all_of(word.begin(), word.end(), ::isdigit))
            row.emplace_back(stoi(word));
        else
            row.emplace_back(word);

        if (row.size() == schema.size()) {
            table.insert(Record(row));
            row.clear();
        }
    }

    return table;
}

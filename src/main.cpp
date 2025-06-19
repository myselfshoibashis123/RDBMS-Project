#include "../include/MiniRDBMS.hpp"
#include <sstream>
#include <map>
#include <algorithm>
#include <fstream>

using namespace std;

int main() {
    map<string, Table> db;
    string command;

    cout << "\nMiniSQL DBMS (Type EXIT to quit)\n";
    while (true) {
        cout << "\nMiniSQL> ";
        getline(cin, command);
        transform(command.begin(), command.end(), command.begin(), ::toupper);

        if (command == "EXIT") break;

        stringstream ss(command);
        string token;
        ss >> token;

        if (token == "CREATE") {
            string temp, tableName;
            ss >> temp >> tableName;
            getline(cin, command);
            vector<Attribute> schema;
            cout << "Define attributes as <name> <INT/STRING>, end with DONE\n";
            while (true) {
                cout << "> ";
                getline(cin, command);
                if (command == "DONE") break;
                stringstream attrStream(command);
                string attrName, attrType;
                attrStream >> attrName >> attrType;
                schema.emplace_back(attrName, attrType == "INT" ? DataType::INT : DataType::STRING);
            }
            db[tableName] = Table(tableName, schema);
            cout << "Table created: " << tableName << endl;
        }
        else if (token == "INSERT") {
            string temp, tableName;
            ss >> temp >> tableName;
            getline(cin, command);
            cout << "Provide values: \n> ";
            getline(cin, command);
            stringstream valStream(command);
            vector<Value> row;
            string val;
            while (valStream >> val) {
                if (all_of(val.begin(), val.end(), ::isdigit))
                    row.emplace_back(stoi(val));
                else
                    row.emplace_back(val);
            }
            db[tableName].insert(Record(row));
        }
        else if (token == "SELECT") {
            string star, from, tableName;
            ss >> star >> from >> tableName;
            db[tableName].print();
        }
        else if (token == "DELETE") {
            string from, tableName, where, col, op, val;
            ss >> from >> tableName >> where >> col >> op >> val;
            Value v = all_of(val.begin(), val.end(), ::isdigit) ? Value(stoi(val)) : Value(val);
            db[tableName].deleteWhere(col, op, v);
        }
        else if (token == "UPDATE") {
            string tableName, setStr, colSet, eq, newVal, where, colWhere, op, val;
            ss >> tableName >> setStr >> colSet >> eq >> newVal >> where >> colWhere >> op >> val;
            Value v = all_of(val.begin(), val.end(), ::isdigit) ? Value(stoi(val)) : Value(val);
            Value nv = all_of(newVal.begin(), newVal.end(), ::isdigit) ? Value(stoi(newVal)) : Value(newVal);
            db[tableName].updateWhere(colWhere, op, v, colSet, nv);
        }
        else if (token == "INTERSECT") {
            string table1, table2;
            ss >> table1 >> table2;
            db[table1].intersect(db[table2]).print();
        }
        else if (token == "UNION") {
            string table1, table2;
            ss >> table1 >> table2;
            db[table1].setUnion(db[table2]).print();
        }
        else if (token == "JOIN") {
            string table1, table2, on, col;
            ss >> table1 >> table2 >> on >> col;
            db[table1].innerJoin(db[table2], col, col).print();
        }
        else if (token == "SAVE") {
            string tableName;
            ss >> tableName;
            ofstream out(tableName + ".db");
            if (out) {
                const Table& t = db[tableName];
                for (const auto& attr : t.schema)
                    out << attr.name << "," << (attr.type == DataType::INT ? "INT" : "STRING") << ";";
                out << endl;
                for (const auto& rec : t.records) {
                    for (const auto& val : rec.values) {
                        if (holds_alternative<int>(val)) out << get<int>(val);
                        else out << get<string>(val);
                        out << ",";
                    }
                    out << endl;
                }
                cout << "Table saved to " << tableName << ".db\n";
            } else {
                cout << "Failed to save table." << endl;
            }
        }
        else if (token == "LOAD") {
            string tableName;
            ss >> tableName;
            ifstream in(tableName + ".db");
            if (in) {
                string line;
                getline(in, line);
                stringstream attrStream(line);
                string part;
                vector<Attribute> schema;
                while (getline(attrStream, part, ';')) {
                    if (part.empty()) continue;
                    stringstream field(part);
                    string name, type;
                    getline(field, name, ',');
                    getline(field, type, ',');
                    schema.emplace_back(name, type == "INT" ? DataType::INT : DataType::STRING);
                }
                Table t(tableName, schema);
                while (getline(in, line)) {
                    stringstream rowStream(line);
                    vector<Value> values;
                    string val;
                    int i = 0;
                    while (getline(rowStream, val, ',')) {
                        if (schema[i].type == DataType::INT)
                            values.emplace_back(stoi(val));
                        else
                            values.emplace_back(val);
                        i++;
                    }
                    t.insert(Record(values));
                }
                db[tableName] = t;
                cout << "Loaded table from " << tableName << ".db\n";
            } else {
                cout << "Failed to load table." << endl;
            }
        }
        else {
            cout << "Unknown command. Try CREATE, INSERT, SELECT, DELETE, UPDATE, INTERSECT, UNION, JOIN, SAVE, LOAD\n";
        }
    }
    return 0;
}

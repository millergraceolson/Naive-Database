// MP4 miller olsonn!
#include "db.hpp"

void Database::CreateTable(const std::string& table_name) {
  tables_[table_name] = new DbTable();
}

void Database::DropTable(const std::string& table_name) {
  if (!tables_.contains(table_name)) {
    throw std::invalid_argument("");
  }
  delete tables_.at(table_name);
  tables_.erase(table_name);
}

DbTable& Database::GetTable(const std::string& table_name) {
  return *tables_[table_name];
}

Database::Database(const Database& rhs) {
  for (const auto& [name, table] : rhs.tables_) {
    tables_[name] = new DbTable(*table);
  }
}

Database& Database::operator=(const Database& rhs) {
  if (this != &rhs) {
    for (auto& [name, table] : tables_) {
      delete table;
    }
    tables_.clear();

    for (const auto& [name, table] : rhs.tables_) {
      tables_[name] = new DbTable(*table);
    }
  }
  return *this;
}

Database::~Database() {
  for (auto table : tables_) {
    delete table.second;
    table.second = nullptr;
  }
  tables_.clear();
}

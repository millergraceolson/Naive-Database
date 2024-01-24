// MP4 miller olsonn!
#include "db_table.hpp"

#include <stdexcept>

#include "iomanip"

const unsigned int kRowGrowthRate = 2;

// helper function for AddColumn:

void DbTable::Resize() {
  for (auto& [id, row] : rows_) {
    void** newrow = new void*[static_cast<unsigned long>(row_col_capacity_) *
                              kRowGrowthRate];
    for (unsigned int i = 0; i < row_col_capacity_; i++) {
      newrow[i] = row[i];
    }
    delete[] row;
    row = newrow;
  }
  row_col_capacity_ *= kRowGrowthRate;
}

void DbTable::AddColumn(const std::pair<std::string, DataType>& col_desc) {
  if (col_descs_.size() == row_col_capacity_) {
    Resize();
  }
  col_descs_.push_back(col_desc);
  DataType type = col_desc.second;

  for (auto& [id, row] : rows_) {
    if (type == DataType::kDouble) {
      row[col_descs_.size() - 1] = static_cast<void*>(new double{0.0});
    } else if (type == DataType::kString) {
      row[col_descs_.size() - 1] = static_cast<void*>(new std::string{""});
    } else if (type == DataType::kInt) {
      row[col_descs_.size() - 1] = static_cast<void*>(new int{0});
    }
  }
}

void DbTable::DeleteColumnByIdx(unsigned int col_idx) {
  if (col_idx >= col_descs_.size()) {
    throw std::out_of_range("");
  }
  if (col_descs_.size() == 1 && !rows_.empty()) {
    throw std::runtime_error("");
  }
  for (auto& [id, row] : rows_) {
    DataType type = col_descs_[col_idx].second;
    if (type == DataType::kString) {
      delete static_cast<std::string*>(row[col_idx]);
    }
    if (type == DataType::kDouble) {
      delete static_cast<double*>(row[col_idx]);
    }
    if (type == DataType::kInt) {
      delete static_cast<int*>(row[col_idx]);
    }
    for (unsigned int i = col_idx + 1; i < col_descs_.size(); i++) {
      row[i - 1] = row[i];
    }
  }
  col_descs_.erase(col_descs_.begin() + col_idx);
}

void DbTable::AddRow(const std::initializer_list<std::string>& col_data) {
  if (col_data.size() != col_descs_.size()) {
    throw std::invalid_argument("");
  }
  rows_[next_unique_id_] = new void*[row_col_capacity_];
  int i = 0;
  for (const auto& data : col_data) {
    DataType type = col_descs_.at(i).second;
    if (type == DataType::kString) {
      rows_[next_unique_id_][i] = static_cast<void*>(new std::string{data});
    }
    if (type == DataType::kDouble) {
      rows_[next_unique_id_][i] =
          static_cast<void*>(new double{std::stod(data)});
    }
    if (type == DataType::kInt) {
      rows_[next_unique_id_][i] = static_cast<void*>(new int{std::stoi(data)});
    }
    i++;
  }
  next_unique_id_++;
}

void DbTable::DeleteRowById(unsigned int id) {
  if (rows_.find(id) == rows_.end()) {
    throw std::out_of_range("");
  }
  void** row_to_delete = rows_[id];
  for (unsigned int i = 0; i < col_descs_.size(); i++) {
    DataType type = col_descs_[i].second;
    switch (type) {
    case DataType::kString:
      delete static_cast<std::string*>(row_to_delete[i]);
      break;
    case DataType::kDouble:
      delete static_cast<double*>(row_to_delete[i]);
      break;
    case DataType::kInt:
      delete static_cast<int*>(row_to_delete[i]);
      break;
    }
  }
  delete[] row_to_delete;
  rows_.erase(id);
}

void DbTable::PrintColumn(std::ostream& os, const DbTable& table) const {
  for (size_t i = 0; i < table.col_descs_.size(); i++) {
    os << table.col_descs_[i].first << "(";
    switch (table.col_descs_[i].second) {
    case DataType::kString:
      os << "std::string";
      break;
    case DataType::kDouble:
      os << "double";
      break;
    case DataType::kInt:
      os << "int";
      break;
    }
    os << ")";
    if (i != table.col_descs_.size() - 1) {
      os << ", ";
    }
  }
  os << std::endl;
}

void DbTable::PrintRow(std::ostream& os,
                       void** /*row*/,
                       const DbTable& table) const {
  for (const auto& [id, row] : table.rows_) {
    for (unsigned int i = 0; i < table.col_descs_.size(); i++) {
      switch (table.col_descs_[i].second) {
      case DataType::kString:
        os << *(static_cast<std::string*>(row[i]));
        break;
      case DataType::kDouble:
        os << *(static_cast<double*>(row[i]));
        break;
      case DataType::kInt:
        os << *(static_cast<int*>(row[i]));
        break;
      }
      if (i != table.col_descs_.size() - 1) {
        os << ", ";
      }
    }
    os << std::endl;
  }
}

std::ostream& operator<<(std::ostream& os, const DbTable& table) {
  for (const auto& col_desc : table.col_descs_) {
    os << col_desc.first << "(";
    switch (col_desc.second) {
    case DataType::kString:
      os << "std::string";
      break;
    case DataType::kDouble:
      os << "double";
      break;
    case DataType::kInt:
      os << "int";
      break;
    }
    os << ")";
    if (&col_desc != &table.col_descs_.back()) {
      os << ", ";
    }
  }
  for (const auto& [id, row] : table.rows_) {
    for (unsigned int i = 0; i < table.col_descs_.size(); i++) {
      switch (table.col_descs_[i].second) {
      case DataType::kString:
        os << *(static_cast<std::string*>(row[i]));
        break;
      case DataType::kDouble:
        os << *(static_cast<double*>(row[i]));
        break;
      case DataType::kInt:
        os << *(static_cast<int*>(row[i]));
        break;
      }
      if (i != table.col_descs_.size() - 1) {
        os << ", ";
      }
    }
    os << std::endl;
  }
  return os;
}

DbTable::DbTable(const DbTable& rhs) {
  next_unique_id_ = rhs.next_unique_id_;
  row_col_capacity_ = rhs.row_col_capacity_;
  col_descs_ = rhs.col_descs_;

  for (const auto& [id, row] : rhs.rows_) {
    void** new_row = new void*[row_col_capacity_];
    for (unsigned int i = 0; i < col_descs_.size(); i++) {
      DataType type = col_descs_[i].second;
      switch (type) {
      case DataType::kDouble:
        new_row[i] = new double{*static_cast<double*>(row[i])};
        break;
      case DataType::kString:
        new_row[i] = new std::string{*static_cast<std::string*>(row[i])};
        break;
      case DataType::kInt:
        new_row[i] = new int{*static_cast<int*>(row[i])};
        break;
      }
    }
    rows_[id] = new_row;
  }
}

// helper
void DbTable::DeleteRow(void** row) {
  for (unsigned int i = 0; i < col_descs_.size(); i++) {
    DataType type = col_descs_[i].second;
    switch (type) {
    case DataType::kString:
      delete static_cast<std::string*>(row[i]);
      break;
    case DataType::kDouble:
      delete static_cast<double*>(row[i]);
      break;
    case DataType::kInt:
      delete static_cast<int*>(row[i]);
      break;
    }
  }
  delete[] row;
}

// helper
void** DbTable::CopyRow(void** src_row) {
  void** new_row = new void*[row_col_capacity_];
  for (unsigned int i = 0; i < col_descs_.size(); i++) {
    DataType type = col_descs_[i].second;
    switch (type) {
    case DataType::kDouble:
      new_row[i] = new double{*static_cast<double*>(src_row[i])};
      break;
    case DataType::kString:
      new_row[i] = new std::string{*static_cast<std::string*>(src_row[i])};
      break;
    case DataType::kInt:
      new_row[i] = new int{*static_cast<int*>(src_row[i])};
      break;
    }
  }
  return new_row;
}

DbTable& DbTable::operator=(const DbTable& rhs) {
  if (this != &rhs) {
    for (auto& [row_id, row] : rows_) {
      DeleteRow(row);
    }
    rows_.clear();

    next_unique_id_ = rhs.next_unique_id_;
    row_col_capacity_ = rhs.row_col_capacity_;
    col_descs_ = rhs.col_descs_;

    for (const auto& [id, row] : rhs.rows_) {
      rows_[id] = CopyRow(row);
    }
  }
  return *this;
}

DbTable::~DbTable() {
  for (auto& [row_id, row] : rows_) {
    for (unsigned int i = 0; i < col_descs_.size(); i++) {
      DataType type = col_descs_[i].second;
      if (type == DataType::kInt) {
        delete static_cast<int*>(row[i]);
      }
      if (type == DataType::kString) {
        delete static_cast<std::string*>(row[i]);
      }
      if (type == DataType::kDouble) {
        delete static_cast<double*>(row[i]);
      }
    }
    delete[] row;
    row = nullptr;
  }
  next_unique_id_ = 0;
  row_col_capacity_ = 2;
}

// LAST Function
// std::ostream& operator<<(std::ostream& os, const DbTable& table) {
// if (table.rows_.empty()) return os;
// return os;
//}

#include <db/Database.hpp>

using namespace db;

BufferPool &Database::getBufferPool() { return bufferPool; }

Database &db::getDatabase() {
  static Database instance;
  return instance;
}

void Database::add(std::unique_ptr<DbFile> file) {
  // TODO pa1: add the file to the catalog. Note that the file must not exist.
  std:: string name = file->getName();
  auto it = catalog.find(name);
  if(it != catalog.end()) {
    throw std::logic_error("Database already exists");
  }

  catalog[name] =std::move(file);

}

std::unique_ptr<DbFile> Database::remove(const std::string &name) {
  // TODO pa1: remove the file from the catalog. Note that the file must exist.
  auto it = catalog.find(name);
  if(it == catalog.end()) {
    throw std::logic_error("File does not exist");
  }
  bufferPool.flushFile(name);
  auto file = std::move(it->second);
  catalog.erase(it);
  return file;
}

DbFile &Database::get(const std::string &name) const {
  // TODO pa1: get the file from the catalog. Note that the file must exist.
  auto it = catalog.find(name);
  if(it == catalog.end()) {
    throw std::logic_error("File does not exist");
  }
  return *it->second;

}

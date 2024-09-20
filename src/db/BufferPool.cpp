#include <db/BufferPool.hpp>
#include <db/Database.hpp>
#include <numeric>
#include <db/DbFile.hpp>
using namespace db;

BufferPool::BufferPool()
: capacity(DEFAULT_NUM_PAGES)
// TODO pa1: add initializations if needed
{

}


BufferPool::~BufferPool() {
  // TODO pa1: flush any remaining dirty pages
  for(const auto &dirtyPage:dirtyPages) {
    if(dirtyPage.second)
      flushPage(dirtyPage.first);
  }
}

Page &BufferPool::getPage(const PageId &pid) {
  // TODO pa1: If already in buffer pool, make it the most recent page and return it

  // TODO pa1: If there are no available pages, evict the least recently used page. If it is dirty, flush it to disk

  // TODO pa1: Read the page from disk to one of the available slots, make it the most recent page
  auto it = pid_to_index.find(pid);
  if(it != pid_to_index.end()) {
    lruList.splice(lruList.begin(),lruList,lruMap[pid]);
    size_t index = pid_to_index[pid];
    return pages[index];
  }
  if(lruList.size() == capacity) {
    auto pid_to_evict = lruList.back();

    if(isDirty(pid_to_evict)) {
      flushPage(pid_to_evict);
    }

    discardPage(pid_to_evict);
  }
  Page newPage;
  DbFile &file  = db::getDatabase().get(pid.file);
  file.readPage(newPage,pid.page);
  size_t new_index = -1;
  for (size_t i=0;i<DEFAULT_NUM_PAGES;i++) {
    if(pages[i][0]=='\0') {
      new_index = i;
      break;
    }
  }
  if(new_index == -1) {
    throw std::runtime_error("No available pages in the buffer pool.");
  }
  pages[new_index] = newPage;
  pid_to_index[pid] = new_index;
  dirtyPages[pid] = false;
  lruList.emplace_front(pid);
  lruMap[pid] = lruList.begin();
  size_t index = pid_to_index[pid];
  return pages[index];
}

void BufferPool::markDirty(const PageId &pid) {
  // TODO pa1: Mark the page as dirty. Note that the page must already be in the buffer pool
  auto it = pid_to_index.find(pid);
  if(it != pid_to_index.end()) {
    dirtyPages[pid] = true;
  }
}
bool BufferPool::isDirty(const PageId &pid) const {
  // TODO pa1: Return whether the page is dirty. Note that the page must already be in the buffer pool
  if (pid_to_index.find(pid) == pid_to_index.end()) {
    throw std::logic_error("Page not found in buffer pool.");
  }
  auto it = dirtyPages.find(pid);
    return it != dirtyPages.end() && it->second;
}
bool BufferPool::contains(const PageId &pid) const {
  // TODO pa1: Return whether the page is in the buffer pool
  return pid_to_index.find(pid) != pid_to_index.end();
}

void BufferPool::discardPage(const PageId &pid) {
  // TODO pa1: Discard the page from the buffer pool. Note that the page must already be in the buffer pool
  auto it = pid_to_index.find(pid);
  if(it != pid_to_index.end()) {
    size_t index = it->second;
    pid_to_index.erase(it);
    dirtyPages.erase(pid);
    lruList.erase(lruMap[pid]);
    lruMap.erase(pid);
    pages[index].fill('\0');
  }
  else {
    throw std::logic_error("Page not found.");
  }
}

void BufferPool::flushPage(const PageId &pid) {
  // TODO pa1: Flush the page to disk. Note that the page must already be in the buffer pool
  auto it = pid_to_index.find(pid);
  if(it != pid_to_index.end()) {
    DbFile &file  = db::getDatabase().get(pid.file);
    size_t index = it->second;
    if(dirtyPages[pid]) {
      file.writePage(pages[index],pid.page);
      dirtyPages[pid] = false;
    }
  }
}

void BufferPool::flushFile(const std::string &file) {
  // TODO pa1: Flush all pages of the file to disk
  for(const auto &dirtyPage:dirtyPages) {
    if(dirtyPage.first.file ==file) {
      flushPage(dirtyPage.first);
    }
  }
}

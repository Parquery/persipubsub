// https://gist.github.com/socantre/b45b3e23e6f1f4715f08

#pragma once

#include <utility>

namespace socantre {

template <typename R, typename D> class unique_resource {
  R resource;
  D deleter;
  bool execute_on_destruction; // exposition only
  unique_resource &operator=(unique_resource const &) = delete;
  unique_resource(unique_resource const &) = delete; // no copies!
 public:
  // construction
  explicit unique_resource(R &&resource, D &&deleter,
                           bool shouldrun = true) noexcept(true)
      : resource(std::move(resource)), deleter(std::move(deleter)),
        execute_on_destruction{shouldrun} {}
  // move
  unique_resource(unique_resource &&other) noexcept(true)
      : resource(std::move(other.resource)), deleter(std::move(other.deleter)),
        execute_on_destruction{other.execute_on_destruction} {
    other.release();
  }
  unique_resource &
  operator=(unique_resource &&other) noexcept(noexcept(this->reset())) {
    this->reset();
    this->deleter = std::move(other.deleter);
    this->resource = std::move(other.resource);
    this->execute_on_destruction = other.execute_on_destruction;
    other.release();
    return *this;
  }
  // resource release
  ~unique_resource() noexcept(noexcept(this->reset())) { this->reset(); }
  void reset() noexcept(noexcept(this->get_deleter()(resource))) {
    if (execute_on_destruction) {
      this->execute_on_destruction = false;
      this->get_deleter()(resource);
    }
  }
  void reset(R &&newresource) noexcept(noexcept(this->reset())) {
    this->reset();
    this->resource = std::move(newresource);
    this->execute_on_destruction = true;
  }
  R const &release() noexcept(true) {
    this->execute_on_destruction = false;
    return this->get();
  }
  // resource access
  R const &get() const noexcept(true) { return this->resource; }
  operator R const &() const noexcept(true) { return this->resource; }
  R operator->() const noexcept(true) { return this->resource; }
  std::add_lvalue_reference_t<std::remove_pointer_t<R>> operator*() const {
    return *this->resource;
  }
  // deleter access
  const D &get_deleter() const noexcept(true) { return this->deleter; }
};
// factories
template <typename R, typename D>
auto make_unique_resource(R &&r, D &&d) noexcept(true)
-> decltype(unique_resource<R, std::remove_reference_t<D>>(
    std::move(r), std::forward<std::remove_reference_t<D>>(d), true)) {
  return unique_resource<R, std::remove_reference_t<D>>(
      std::move(r), std::forward<std::remove_reference_t<D>>(d), true);
}
template <typename R, typename D>
auto make_unique_resource_checked(R r, R invalid, D d) noexcept(true)
-> decltype(unique_resource<R, D>(std::move(r), std::move(d), true)) {
  bool shouldrun = not bool(r == invalid);
  return unique_resource<R, D>(std::move(r), std::move(d), shouldrun);
}

}  // namespace socantre

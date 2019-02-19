// https://gist.github.com/socantre/b45b3e23e6f1f4715f08
#include <utility>

#pragma once

namespace socantre {

template <typename EF> struct scope_exit {
  // construction
  explicit scope_exit(EF &&f) noexcept(true)
      : exit_function(std::move(f)), execute_on_destruction{true} {}
  // move
  scope_exit(scope_exit &&rhs) noexcept(true)
      : exit_function(std::move(rhs.exit_function)),
        execute_on_destruction{rhs.execute_on_destruction} {
    rhs.release();
  }
  // release
  ~scope_exit() noexcept(noexcept(this->exit_function())) {
    if (execute_on_destruction)
      this->exit_function();
  }
  void release() noexcept(true) { this->execute_on_destruction = false; }

private:
  scope_exit(scope_exit const &) = delete;
  void operator=(scope_exit const &) = delete;
  scope_exit &operator=(scope_exit &&) = delete;
  EF exit_function;
  bool execute_on_destruction; // exposition only
};

template <typename EF>
auto make_scope_exit(EF &&exit_function) noexcept(true) -> decltype(
    scope_exit<std::remove_reference_t<EF>>(std::forward<EF>(exit_function))) {
  return scope_exit<std::remove_reference_t<EF>>(
      std::forward<EF>(exit_function));
}
}  // namespace socantre

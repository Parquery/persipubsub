#For the shared library:
set ( PROJECT_LINK_LIBS liblmdb.so)
link_directories( /usr/local/lib)

add_library(persipubsub SHARED queue.cpp queue.h library.h library.cpp filesystem.cpp)
target_link_libraries( persipubsub ${PROJECT_LINK_LIBS})

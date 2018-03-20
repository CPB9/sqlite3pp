project('sqlite3pp', ['c', 'cpp'], default_options : ['cpp_std=c++11', 'c_std=c99'])

bmcl_mod = subproject('bmcl', default_options: ['build_tests=false'])
bmcl_dep = bmcl_mod.get_variable('bmcl_dep')
thread_dep = dependency('threads')

deps = [bmcl_dep, thread_dep]

if target_machine.system() != 'windows'
  dl_lib = meson.get_compiler('c').find_library('dl')
  deps += dl_lib
endif

src = [
  'src/sqlite3.c',
  'src/sqlite3.h',
  'src/sqlite3pp.cpp',
  'src/sqlite3pp.h',
  'src/sqlite3ppext.cpp',
  'src/sqlite3ppext.h',
  'src/sqlite3ext.h',
]

inc = include_directories('src')

sqlite3pp_lib = static_library('sqlite3pp',
  sources : src,
  include_directories : inc,
  dependencies : deps,
)

sqlite3pp_dep = declare_dependency(
  link_with : sqlite3pp_lib,
  include_directories : inc,
  dependencies : deps,
)

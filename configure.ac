AC_INIT([melian], [0.1.0], [https://github.com/xsawyerx/melian/issues], [melian])
AC_CONFIG_SRCDIR([server/melian-server.c])
AC_CONFIG_AUX_DIR([build-aux])
AM_INIT_AUTOMAKE([foreign subdir-objects])

AC_CANONICAL_HOST
AC_USE_SYSTEM_EXTENSIONS

AC_PROG_CC
AC_PROG_INSTALL

case $host_os in
  linux*)
    PLATFORM_CPPFLAGS="-D_GNU_SOURCE -D_XOPEN_SOURCE"
    ;;
  *)
    PLATFORM_CPPFLAGS=""
    ;;
esac
AC_SUBST([PLATFORM_CPPFLAGS])

AC_ARG_WITH([mysql],
  [AS_HELP_STRING([--with-mysql=PREFIX],
    [Prefix where MySQL/MariaDB client headers and libraries can be found (auto-detect by default)])],
  [],
  [with_mysql=auto])

have_mysql=no
MYSQL_CFLAGS=""
MYSQL_LIBS=""
if test "x$with_mysql" != "xno"; then
  case $with_mysql in
    auto|yes)
      MYSQL_CPPFLAGS_CAND=""
      MYSQL_LDFLAGS_CAND=""
      ;;
    *)
      MYSQL_CPPFLAGS_CAND="-I$with_mysql/include"
      MYSQL_LDFLAGS_CAND="-L$with_mysql/lib"
      ;;
  esac

  save_CPPFLAGS="$CPPFLAGS"
  CPPFLAGS="$MYSQL_CPPFLAGS_CAND $CPPFLAGS"
  AC_CHECK_HEADER([mysql/mysql.h],
    [have_mysql=yes],
    [have_mysql=no])
  CPPFLAGS="$save_CPPFLAGS"

  if test "x$have_mysql" = "xyes"; then
    save_LDFLAGS="$LDFLAGS"
    LDFLAGS="$MYSQL_LDFLAGS_CAND $LDFLAGS"
    AC_CHECK_LIB([mysqlclient], [mysql_init],
      [MYSQL_LIBS="$MYSQL_LDFLAGS_CAND -lmysqlclient"],
      [have_mysql=no])
    LDFLAGS="$save_LDFLAGS"
  fi

  if test "x$have_mysql" = "xyes"; then
    MYSQL_CFLAGS="$MYSQL_CPPFLAGS_CAND"
    AC_DEFINE([HAVE_MYSQL], [1], [Define if MySQL client support is available])
  elif test "x$with_mysql" = "xyes"; then
    AC_MSG_ERROR([MySQL support requested but mysql/mysql.h or libmysqlclient not found])
  fi
fi
AC_SUBST([MYSQL_CFLAGS])
AC_SUBST([MYSQL_LIBS])

# libevent is mandatory (server networking)
AC_ARG_WITH([libevent],
  [AS_HELP_STRING([--with-libevent=PREFIX],
    [Prefix where libevent headers and libraries can be found])],
  [],
  [with_libevent=auto])

case $with_libevent in
  no)
    AC_MSG_ERROR([libevent is required to build melian])
    ;;
  auto|yes)
    LIBEVENT_CPPFLAGS=""
    LIBEVENT_LDFLAGS=""
    ;;
  *)
    LIBEVENT_CPPFLAGS="-I$with_libevent/include"
    LIBEVENT_LDFLAGS="-L$with_libevent/lib"
    ;;
esac

save_CPPFLAGS="$CPPFLAGS"
CPPFLAGS="$LIBEVENT_CPPFLAGS $CPPFLAGS"
AC_CHECK_HEADER([event2/event.h], [],
  [AC_MSG_ERROR([event2/event.h not found; install libevent >= 2.1 or use --with-libevent=PREFIX])])
CPPFLAGS="$save_CPPFLAGS"

save_LDFLAGS="$LDFLAGS"
LDFLAGS="$LIBEVENT_LDFLAGS $LDFLAGS"
AC_CHECK_LIB([event], [event_base_new],
  [LIBEVENT_LIBS="$LIBEVENT_LDFLAGS -levent"],
  [AC_MSG_ERROR([libevent not found; install libevent >= 2.1 or use --with-libevent=PREFIX])])
LDFLAGS="$save_LDFLAGS"
AC_SUBST([LIBEVENT_LIBS])

# Optional SQLite support (prepare to link when available)
AC_ARG_WITH([sqlite3],
  [AS_HELP_STRING([--with-sqlite3=PREFIX],
    [Prefix where SQLite3 headers and libraries can be found (auto-detect by default)])],
  [],
  [with_sqlite3=auto])

have_sqlite3=no
SQLITE_CFLAGS=""
SQLITE_LIBS=""
if test "x$with_sqlite3" != "xno"; then
  case $with_sqlite3 in
    auto|yes)
      SQLITE_CPPFLAGS_CAND=""
      SQLITE_LDFLAGS_CAND=""
      ;;
    *)
      SQLITE_CPPFLAGS_CAND="-I$with_sqlite3/include"
      SQLITE_LDFLAGS_CAND="-L$with_sqlite3/lib"
      ;;
  esac

  save_CPPFLAGS="$CPPFLAGS"
  CPPFLAGS="$SQLITE_CPPFLAGS_CAND $CPPFLAGS"
  AC_CHECK_HEADER([sqlite3.h],
    [have_sqlite3=yes],
    [have_sqlite3=no])
  CPPFLAGS="$save_CPPFLAGS"

  if test "x$have_sqlite3" = "xyes"; then
    save_LDFLAGS="$LDFLAGS"
    LDFLAGS="$SQLITE_LDFLAGS_CAND $LDFLAGS"
    AC_CHECK_LIB([sqlite3], [sqlite3_open],
      [SQLITE_LIBS="$SQLITE_LDFLAGS_CAND -lsqlite3"],
      [have_sqlite3=no])
    LDFLAGS="$save_LDFLAGS"
  fi

  if test "x$have_sqlite3" = "xyes"; then
    SQLITE_CFLAGS="$SQLITE_CPPFLAGS_CAND"
    AC_DEFINE([HAVE_SQLITE3], [1], [Define if SQLite3 support is available])
  elif test "x$with_sqlite3" = "xyes"; then
    AC_MSG_ERROR([SQLite3 support requested but sqlite3.h or libsqlite3 not found])
  fi
fi
AC_SUBST([SQLITE_CFLAGS])
AC_SUBST([SQLITE_LIBS])

if test "x$have_mysql" != "xyes" && test "x$have_sqlite3" != "xyes"; then
  AC_MSG_ERROR([No supported database drivers found. Install MySQL client and/or SQLite3, or use --with-mysql/--with-sqlite3 to point to their prefixes.])
fi

default_db_driver="none"
AS_IF([test "x$have_mysql" = "xyes"], [default_db_driver="mysql"])
AS_IF([test "x$have_sqlite3" = "xyes" && test "x$default_db_driver" = "xnone"],
      [default_db_driver="sqlite"])

AC_MSG_NOTICE([])
AC_MSG_NOTICE([Database backend configuration:])
AC_MSG_NOTICE([  MySQL : $have_mysql])
AC_MSG_NOTICE([  SQLite: $have_sqlite3])
AC_MSG_NOTICE([  Default driver: $default_db_driver])
AC_MSG_NOTICE([])

# libjansson is mandatory for the client
AC_ARG_WITH([jansson],
  [AS_HELP_STRING([--with-jansson=PREFIX],
    [Prefix where libjansson headers and libraries can be found])],
  [],
  [with_jansson=auto])

case $with_jansson in
  no)
    AC_MSG_ERROR([libjansson is required to build the melian client])
    ;;
  auto|yes)
    JANSSON_CPPFLAGS=""
    JANSSON_LDFLAGS=""
    ;;
  *)
    JANSSON_CPPFLAGS="-I$with_jansson/include"
    JANSSON_LDFLAGS="-L$with_jansson/lib"
    ;;
esac

save_CPPFLAGS="$CPPFLAGS"
CPPFLAGS="$JANSSON_CPPFLAGS $CPPFLAGS"
AC_CHECK_HEADER([jansson.h], [],
  [AC_MSG_ERROR([jansson.h not found; install libjansson or use --with-jansson=PREFIX])])
CPPFLAGS="$save_CPPFLAGS"

save_LDFLAGS="$LDFLAGS"
LDFLAGS="$JANSSON_LDFLAGS $LDFLAGS"
AC_CHECK_LIB([jansson], [json_loads],
  [JANSSON_LIBS="$JANSSON_LDFLAGS -ljansson"],
  [AC_MSG_ERROR([libjansson not found; install libjansson or use --with-jansson=PREFIX])])
LDFLAGS="$save_LDFLAGS"
AC_SUBST([JANSSON_LIBS])

AC_CHECK_LIB([pthread], [pthread_create],
  [PTHREAD_LIBS="-lpthread"],
  [AC_MSG_ERROR([pthread library not found])])
AC_SUBST([PTHREAD_LIBS])

AC_CHECK_LIB([m], [cos],
  [LIBM="-lm"],
  [LIBM=""])
AC_SUBST([LIBM])

AC_CONFIG_FILES([Makefile])
AC_OUTPUT

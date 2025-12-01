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
    [Prefix where MySQL/MariaDB client headers and libraries can be found])],
  [],
  [with_mysql=yes])

case $with_mysql in
  no)
    AC_MSG_ERROR([MySQL/MariaDB client support is required to build melian])
    ;;
  yes|auto)
    MYSQL_CPPFLAGS=""
    MYSQL_LDFLAGS=""
    ;;
  *)
    MYSQL_CPPFLAGS="-I$with_mysql/include"
    MYSQL_LDFLAGS="-L$with_mysql/lib"
    ;;
esac

save_CPPFLAGS="$CPPFLAGS"
CPPFLAGS="$MYSQL_CPPFLAGS $CPPFLAGS"
AC_CHECK_HEADER([mysql/mysql.h], [],
  [AC_MSG_ERROR([mysql/mysql.h header not found; use --with-mysql=PREFIX])])
CPPFLAGS="$save_CPPFLAGS"

save_LDFLAGS="$LDFLAGS"
LDFLAGS="$MYSQL_LDFLAGS $LDFLAGS"
AC_CHECK_LIB([mysqlclient], [mysql_init],
  [MYSQL_LIBS="$MYSQL_LDFLAGS -lmysqlclient"],
  [AC_MSG_ERROR([libmysqlclient not found; use --with-mysql=PREFIX])])
LDFLAGS="$save_LDFLAGS"
MYSQL_CFLAGS="$MYSQL_CPPFLAGS"
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

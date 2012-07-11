Summary: A module for PHP applications that visit Taobao TFS Server.
Name: php_tfs_client
Version: 1.1.1
Release: 2%{?dist}
License: Taobao Liscense
Group: Development/Languages

Source0: %{name}-%{version}.tar.gz

BuildRoot: %{_tmppath}/%{name}-root

Requires: php >= 5.2
BuildRequires: php-devel >= 5.2
BuildRequires: boost-devel >= 1.32.0

%description
php_tfs_client is a client to taobao tfs server, It writed by TBS team.
The module perfect file put/get/state/remove function.

%prep
%setup -q

%build
make -f Makefile.opt

%install
[ "$RPM_BUILD_ROOT" != "/" ] && rm -rf $RPM_BUILD_ROOT
install -d $RPM_BUILD_ROOT%{_sysconfdir}/tfs_client
install -d $RPM_BUILD_ROOT%{_libdir}/php/modules
install -d $RPM_BUILD_ROOT%{_sysconfdir}/php.d
install -m 644 tfs.conf $RPM_BUILD_ROOT%{_sysconfdir}/tfs_client/tfs.conf
install -m 644 tfs.conf.test $RPM_BUILD_ROOT%{_sysconfdir}/tfs_client/tfs.conf.test
install -m 644 tfs.conf.colo $RPM_BUILD_ROOT%{_sysconfdir}/tfs_client/tfs.conf.colo
install -m 755 modules/tfs_client.so $RPM_BUILD_ROOT%{_libdir}/php/modules/tfs_client.so
install -m 644 tfs_client.ini $RPM_BUILD_ROOT%{_sysconfdir}/php.d/tfs_client.ini

%files
%defattr(-, root, root)
%dir %{_sysconfdir}/tfs_client
%config(noreplace) %{_sysconfdir}/tfs_client/tfs.conf
%config(noreplace) %{_sysconfdir}/tfs_client/tfs.conf.test
%config(noreplace) %{_sysconfdir}/tfs_client/tfs.conf.colo
%config(noreplace) %{_sysconfdir}/php.d/tfs_client.ini
%{_libdir}/php/modules/tfs_client.so

%changelog


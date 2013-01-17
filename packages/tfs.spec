Name: %NAME
Summary: Taobao file system 
Version: %VERSION
Release: 1%{?dist}
Group: Application
URL: http:://yum.corp.alimama.com
Packager: taobao<opensource@taobao.com>
License: GPL
Vendor: TaoBao
Prefix:%{_prefix}
Source:%{NAME}-%{VERSION}.tar.gz
BuildRoot: %{_tmppath}/%{name}-root

BuildRequires: t-csrd-tbnet-devel >= 1.0.6
BuildRequires: MySQL-devel-community = 5.1.48 
BuildRequires: tair-devel >= 2.3 
BuildRequires: boost-devel >= 1.3 
BuildRequires: readline-devel
BuildRequires: ncurses-devel
BuildRequires: google-perftools >= 1.3 

%define __os_install_post %{nil}
%define debug_package %{nil}

%description
TFS is a distributed file system.

%package devel
Summary: tfs c++ client library
Group: Development/Libraries

%description devel
The %name-devel package contains libraries and header
files for developing applications that use the %name package.

%prep
%setup

%build
chmod u+x build.sh
./build.sh init
#./configure --prefix=%{_prefix}
./configure --prefix=%{_prefix} --enable-uniquestore --enable-taircache --with-tair-root=/opt/csr/tair-2.3
make %{?_smp_mflags}

%install
rm -rf $RPM_BUILD_ROOT
make DESTDIR=$RPM_BUILD_ROOT install

%clean
rm -rf $RPM_BUILD_ROOT

%post
echo %{_prefix}/lib > /etc/ld.so.conf.d/tfs-%{VERSION}.conf
echo /opt/csr/common/lib >> /etc/ld.so.conf.d/tfs-%{VERSION}.conf
/sbin/ldconfig

%post devel
echo %{_prefix}/lib > /etc/ld.so.conf.d/tfs-%{VERSION}.conf
echo /opt/csr/common/lib >> /etc/ld.so.conf.d/tfs-%{VERSION}.conf
/sbin/ldconfig

%postun
rm  -f /etc/ld.so.conf.d/tfs-%{VERSION}.conf

%files
%defattr(0755, admin, admin)
%{_prefix}
%{_prefix}/bin
%{_prefix}/lib
%{_prefix}/conf
%{_prefix}/scripts
%{_prefix}/logs
%{_prefix}/sql

%files devel
%{_prefix}/include
%{_prefix}/lib/libtfsclient.*

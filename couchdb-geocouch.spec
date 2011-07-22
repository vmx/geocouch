%define couchdb_user couchdb
%define couchdb_group couchdb
%define couchver 1.0.2
%define couchlib /usr/lib64/erlang/lib/couch-%{couchver}/
%define couchinc %{couchlib}include/
%define couchbin %{couchlib}ebin/

%define srcname geocouch

Name:           couchdb-%{srcname}
Version:        0.1.2
Release:        2011031001%{?dist}
Summary:        A spatial index for CouchDB

License:        ASL 2.0
URL:            https://github.com/couchbase/geocouch
Source0:        %{srcname}-%{version}.tar.bz2

BuildRequires:  couchdb
Requires:       couchdb

%description
GeoCouch is a spatial extension for Apache CouchDB.


%prep
%setup -q -n %{srcname}-%{version}


%build
make %{?_smp_mflags} COUCH_SRC=%{couchinc}


%install
rm -rf $RPM_BUILD_ROOT
make

# install the configuration file
install -D -m 644 etc/couchdb/local.d/%{srcname}.ini $RPM_BUILD_ROOT%{_sysconfdir}/couchdb/local.d/%{srcname}.ini

# copy the Futon tests
install -d $RPM_BUILD_ROOT%{_datadir}/couchdb/www/script/test/
install -D -m 644 share/www/script/test/* $RPM_BUILD_ROOT%{_datadir}/couchdb/www/script/test/

# modify couchdb tests to include these ones too
install -D -m 644 couch_tests.js $RPM_BUILD_ROOT%{_datadir}/couchdb/www/script/

# compiled beam files
install -d $RPM_BUILD_ROOT%{couchbin}
install -D -m 644 build/*.beam $RPM_BUILD_ROOT%{couchbin}


%files
%doc README.md couch_tests.js
%config(noreplace) %attr(0644, %{couchdb_user}, root) %{_sysconfdir}/couchdb/local.d/%{srcname}.ini
%{_datadir}/couchdb/www/script/
%{couchbin}


%changelog
* Thu Jul 21 2011 Pau Aliagas <linuxnow@gmail.com> 0.1.2-2011031001
- Initial version

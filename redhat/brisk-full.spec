%global username cassandra

%define relname %{name}-%{version}
%define cass_name apache-cassandra-%{version}
%define briskname brisk

Name:           brisk-full
Version:        1beta1
Release:        1
Summary:        Meta RPM for full installation of the DataStax Brisk platform

Group:          Development/Libraries
License:        Apache Software License
URL:            http://www.datastax.com/products/brisk
Source0:        brisk-src.tar.gz
BuildRoot:      %{_tmppath}/%{relname}-root-%(%{__id_u} -n)

BuildRequires: java-devel
BuildRequires: jpackage-utils
BuildRequires: ant
BuildRequires: ant-nodeps

Requires:      brisk-libcassandra
Requires:      brisk-libhadoop
Requires:      brisk-libhive

BuildArch:     noarch

%description
Realtime analytics and distributed database (cassandra libraries)
Brisk is a realtime analytics system marrying the distributed database
Cassandra and the mapreduce system Hadoop together.

This package contains the full Brisk distribution.

Homepage: http://www.datastax.com/products/brisk

%prep
# tmp hack for now, until we figure out a src target
%setup -q -n brisk

%build

%install
mkdir -p %{buildroot}/etc/brisk/default.conf
mkdir -p %{buildroot}/usr/bin/

cp -p packaging-common/brisk-env.sh %{buildroot}/etc/brisk/default.conf
cp -p bin/brisk %{buildroot}/usr/bin/

%clean
%{__rm} -rf %{buildroot}

%files
%defattr(-,root,root,0755)
%attr(755,root,root) %{_bindir}/brisk
%attr(755,%{username},%{username}) %config(noreplace) /%{_sysconfdir}/brisk

%post
alternatives --install /etc/%{briskname} brisk /etc/%{briskname}/default.conf/ 0
exit 0

%postun
# only delete alternative on removal, not upgrade
if [ "$1" = "0" ]; then
    alternatives --remove brisk /etc/%{briskname}/default.conf/
fi
exit 0

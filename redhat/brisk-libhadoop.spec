%global username cassandra

%define relname %{name}-%{version}
%define hadoop_name hadoop-%{version}
%define briskname brisk

Name:           brisk-libhadoop
Version:        0.20.203
Release:        1
Summary:        Repackaging of Apache Hadoop libraries for inclusion in Brisk

Group:          Development/Libraries
License:        Apache Software License
URL:            http://hadoop.apache.org/
Source0:        brisk-src.tar.gz
BuildRoot:      %{_tmppath}/%{relname}-root-%(%{__id_u} -n)

BuildRequires: java-devel
BuildRequires: jpackage-utils
BuildRequires: ant
BuildRequires: ant-nodeps

Requires:      java >= 1.6.0
Requires:      jpackage-utils
Requires(pre): user(cassandra)
Requires(pre): group(cassandra)
Requires(pre): shadow-utils
Provides:      user(cassandra)
Provides:      group(cassandra)

BuildArch:      noarch

%description
realtime analytics and distributed database (hadoop libraries)
Brisk is a realtime analytics system marrying the distributed database
Cassandra and the mapreduce system Hadoop together.

This package contains the Brisk Hadoop libraries.

Homepage: http://www.datastax.com/products/brisk

%prep
# tmp hack for now, until we figure out a src target
%setup -q -n brisk
#%setup -q -n %{relname}-src

%build
ant clean jar -Drelease=true

%install
%{__rm} -rf %{buildroot}
mkdir -p %{buildroot}/etc/brisk
mkdir -p %{buildroot}/usr/share/%{briskname}/hadoop/bin
mkdir -p %{buildroot}/usr/share/%{briskname}/hadoop/lib
mkdir -p %{buildroot}/usr/share/%{briskname}/hadoop/default.conf

# copy over configurations and libs
cp -p resources/hadoop/conf/* %{buildroot}/usr/share/%{briskname}/hadoop/default.conf
cp -p resources/hadoop/*.jar %{buildroot}/usr/share/%{briskname}/hadoop/lib
cp -p resources/hadoop/lib/*.jar %{buildroot}/usr/share/%{briskname}/hadoop/lib
# copy the hadoop binary
cp -p resources/hadoop/bin/* %{buildroot}/usr/share/brisk/hadoop/bin

%clean
%{__rm} -rf %{buildroot}

# still just the user cassandra for now if it does not exist
%pre
getent group %{username} >/dev/null || groupadd -r %{username}
getent passwd %{username} >/dev/null || \
useradd -d /usr/share/%{briskname}/%{username} -g %{username} -M -r %{username}
exit 0


%files
%defattr(-,root,root,0755)
# do we need a %doc task?
%attr(755,root,root) %config /etc/brisk
%attr(755,%{username},%{username}) %config(noreplace) /usr/share/%{briskname}/hadoop/default.conf

# chown on brisk as cassandra is our only user for now
%attr(755,%{username},%{username}) /usr/share/%{briskname}*

%post
alternatives --install /etc/%{briskname}/hadoop hadoop /usr/share/%{briskname}/hadoop/default.conf/ 0
# symlink bin files
ln -s /usr/share/brisk/hadoop/bin/hadoop /usr/bin/hadoop
ln -s /usr/share/brisk/hadoop/bin/hadoop-config.sh /usr/bin/hadoop-config.sh
exit 0

%postun
# only delete alternative on removal, not upgrade
if [ "$1" = "0" ]; then
    alternatives --remove hadoop /usr/share/%{briskname}/hadoop/default.conf/
fi
exit 0



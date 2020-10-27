#!/bin/bash -e

bs_home="$HOME/.ant/bootstrap"

ant_version="1.10.9"
ivy_version="2.5.0"
ant_file="apache-ant-${ant_version}-bin.tar.bz2"
ivy_file="apache-ivy-${ivy_version}-bin.tar.gz"
ant_url="https://downloads.apache.org/ant/binaries/${ant_file}"
ivy_url="https://downloads.apache.org/ant/ivy/${ivy_version}/${ivy_file}"

mkdir -p "$HOME/.ant/"{home,lib,bootstrap}

[[ ! -r "$bs_home/$ant_file" ]] && { echo -n 'downloading and installing ant...' && curl -so "$bs_home/$ant_file" "$ant_url" && tar -C $bs_home -jxf "$bs_home/$ant_file" && echo ' done.' || { echo "ERROR (ant)"; exit 1; } }
[[ ! -r "$bs_home/$ivy_file" ]] && { echo -n 'downloading and installing ivy...' && curl -so "$bs_home/$ivy_file" "$ivy_url" && tar -C $bs_home -xxf "$bs_home/$ivy_file" && echo ' done.' || { echo "ERROR (ivy)"; exit 1; } }

ln -sf "$bs_home/apache-ant-${ant_version}"/{lib,bin} "$HOME/.ant/home"
ln -sf "$bs_home/apache-ivy-${ivy_version}/ivy-${ivy_version}.jar" "$HOME/.ant/lib"
ln -sf "$bs_home/apache-ivy-${ivy_version}/ivy-${ivy_version}.jar" "$HOME/.ant/home/lib"

cat > "$HOME/.ant/ivysettings.xml" <<EOF
<?xml version="1.0" encoding="ISO-8859-1"?>
<ivysettings>
	<include url="\${ivy.default.settings.dir}/ivysettings.xml"/>
	<credentials host="jars.law.di.unimi.it" realm="Sonatype Nexus Repository Manager" username="lawdevel" passwd="l4w"/>
	<resolvers>
		<ibiblio name="sonatype" m2compatible="true" root="https://oss.sonatype.org/content/repositories/public/"/>
		<ibiblio name="law-snapshots" m2compatible="true" root="http://jars.law.di.unimi.it/content/repositories/snapshots" checkmodified="true" changingPattern=".*-SNAPSHOT"/>
		<ibiblio name="law-public" m2compatible="true" root="http://jars.law.di.unimi.it/content/repositories/public/"/>
		<chain name="default" returnFirst="true">
			<resolver ref="local"/>
			<resolver ref="law-snapshots"/>
			<resolver ref="law-public"/>
			<resolver ref="sonatype"/>
			<resolver ref="main"/>
		</chain>
	</resolvers>
	<caches defaultCacheDir="\${user.home}/.ivy/cache" />
</ivysettings>
EOF

cat <<EOF
Please add

	export ANT_HOME="$HOME/.ant/home"
	export PATH="\$ANT_HOME/bin:\$PATH"
	export LOCAL_IVY_SETTINGS="$HOME/.ant/ivysettings.xml"

to your .bash_profile and remember to point your Eclipse IvyIDE plugin to the settings in

	$HOME/.ant/ivysettings.xml

containing the configuration (and password) to use jars.law.di.unimi.it as a primary jar repo.
EOF

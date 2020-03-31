#!/usr/bin/perl

 use DBI;
use strict;

use constant PERSONS => 100;
use constant DATES => 100;
use constant TRANSACTIONS => 50;

 use constant NAMES => [
 'Alexander',
 'Alf',
 'Anders',
 'Andreas',
 'Arild',
   'Arne',
   'Arvid',
   'Asbj',
   'Bjarne',
     'Bj',
     'Christian',
     'Dag',
     'Daniel',
     'Egil',
     'Einar',
     'Eirik',
     'Eivind',
     'Erik',
     'Erling',
     'Espen',
     'Finn',
     'Frank',
     'Fredrik',
     'Frode',
     'Geir',
     'Gunnar',
     'Hans',
     'Harald',
     'Helge',
     'Henrik',
     'Ivar',
       'Jan',
       'Jarle',
       'Jens',
       'Johan',
       'Johannes',
       'John',
       'Jon',
       'Jonas',
       'Jostein',
       'Karl',
       'Kenneth',
       'Kim',
         'Kjell',
	 'Kjetil',
	   'Knut',
	   'Kristian',
	   'Kristoffer',
	     'Lars',
	     'Leif',
	     'Magne',
	     'Magnus',
	     'Marius',
	     'Martin',
	     'Morten',
	     'Nils',
	     'Odd',
	     'Oddvar',
	     'Ola',
	     'Olav',
	       'Ole',
	       'Ove',
	         'Per',
		 'Petter',
		 'Reidar',
		 'Roar',
		 'Robert',
		 'Roger',
		 'Rolf',
		 'Roy',
		 'Rune',
		 'Sigurd',
		 'Simen',
		 'Sindre',
		 'Stein',
		 'Steinar',
		 'Stian',
		 'Stig',
		   'Svein',
		   'Sverre',
		   'Terje',
		   'Thomas',
		   'Thor',
		   'Tom',
		   'Tommy',
		   'Tor',
		   'Torbj',
		   'Tore',
		   'Trond',
		   'Trygve',
		   'Vegard',
		   'Vidar',

 ];

 use constant SEX => ['F', 'M'];
use constant FIELDS => {
	name => {
		probability => 10,
		function => sub {getRandName() . ' ' . getRandName()}
	},
	pledge => {
		probability => 20,
		function => sub {
			my $oldValue = shift;
		       $oldValue->{owner} = getRandName() . ' ' . getRandName();
			rand()*1000000;
		}
	},
	sex => {
		probability => 5,
		function => sub {my $oldValue = shift; $oldValue->{sex} eq 'F' ? 'M' : 'F'}
	},
	species => {
		probability => 50,
		function => sub {"A" x sprintf("%d", rand()*7)}
	},
	death => {
		probability => 10,
		function => sub {
			my $oldValue = shift; 
			if ($oldValue->{death}) {
				return $oldValue->{death};
			} else {
				return rand()*10 < 3 ? DateTime->from_epoch(epoch => $oldValue->{birth}->epoch() + sprintf("%d", rand() * (DateTime->now()->epoch() - $oldValue->{birth}->epoch()))) : undef;
			}
		},
	}
};


 use DateTime;

 my $dbh = DBI->connect("DBI:mysql:anna", $ARGV[0], $ARGV[1]);

 my $sth = $dbh->prepare("
	CREATE TABLE IF NOT EXISTS faces (inn INT, name VARCHAR(20), owner VARCHAR(20),
	       species VARCHAR(20), pledge FLOAT, sex CHAR(1), birth DATE, death DATE, date_ins DATE);
	 ");

 $sth->execute;
 $sth = $dbh->prepare("
	DELETE FROM TABLE faces;
	 ");

 $sth->execute;

my $sql = "INSERT INTO faces (inn, name, owner, species, pledge, sex, birth, death, date_ins)
       VALUES(?,?,?,?,?,?,?,?,?);";
        
my $sth_ins = $dbh->prepare($sql);

my $innHash = {};
my $innArray = [];

for (my $i = 0; $i<&PERSONS; $i++) {
	my $pledge = rand()*2 < 1 ? 0 : rand()*1000000;
	my $birth = DateTime->from_epoch(epoch => DateTime->now()->epoch()-sprintf("%d", rand()*90*365*24*60*60)-20*265*24*60*60);
	my $inn = sprintf("%d", rand() * 10000000000);
	$innArray->[$i] = $inn;
	$innHash->{$inn} = {
		name => getRandName() . ' ' . getRandName(),
		sex => &SEX->[sprintf("%d", rand()*2)],
		birth => $birth,
		pledge => $pledge,
		owner => $pledge > 0 ? getRandName() . ' ' . getRandName() : undef,
		death => rand()*10 < 3 ? DateTime->from_epoch(epoch => $birth->epoch() + sprintf("%d", rand() * DateTime->now()->epoch()) - $birth->epoch()) : undef,
	};
	if ($sth_ins->execute(
		$inn,
		$innHash->{$inn}->{name},
		$innHash->{$inn}->{owner},
		$innHash->{$inn}->{spacies},
		$innHash->{$inn}->{pledge},
		$innHash->{$inn}->{sex},
		$innHash->{$inn}->{birth},
		$innHash->{$inn}->{death},
		DateTime->from_epoch(epoch => DateTime->now()->epoch() - &DATES * 24 * 60 * 60),
	)){
		print "insert data for $innHash->{$inn}->{name}\n";
	}
 } 

 for (my $i = 0; $i < &DATES; $i++) {
 	for (my $j = 0; $j<&TRANSACTIONS; $j++) {
	 	my $inn = $innArray->[sprintf("%d", rand()*(&PERSONS-1))];
		my $changeField;
		foreach my $field (keys %{&FIELDS}) {
			if (rand() * 100 < &FIELDS->{$field}->{probability}){
				$changeField = $field;
				last;
			}
		}

		if ($changeField) {
			$innHash->{$inn}->{$changeField} = &FIELDS->{$changeField}->{function}($innHash->{$inn});
		}

		if ($sth_ins->execute(
			$inn,
			$innHash->{$inn}->{name},
			$innHash->{$inn}->{owner},
			$innHash->{$inn}->{spacies},
			$innHash->{$inn}->{pledge},
			$innHash->{$inn}->{sex},
			$innHash->{$inn}->{birth},
			$innHash->{$inn}->{death},
			DateTime->from_epoch(epoch => DateTime->now()->epoch() - &DATES-$i * 24 * 60 * 60),
		)){
			print "insert data for $innHash->{$inn}->{name}\n";
		}
	}
 }


 sub getRandName{
	 return &NAMES->[sprintf( "%d", rand() * 90)];
 }

 1;
	

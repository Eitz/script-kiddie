#!/usr/bin/env perl 

use strict;
use warnings;
use DBI;

################
#### CONFIG ####
################

# Where to get the data
my %p_from = (
    db  => 'dbname',
    host => '',
    user => '',
    pass => ''
);

# Where to store the data
my %p_to = (
    db  => 'dbname',
    host => '127.0.0.1',
    user => '',
    pass => ''
);

# undef = no limit, save everything
my $limit = 50;

my $valid_config = $p_from{db} && $p_from{host} && $p_from{user} && defined $p_from{pass} && $p_to{db} && $p_to{host} && $p_to{user} && defined $p_to{pass};

unless ($valid_config) {
  print "Configuração inválida, abra o script e corrija.";
}

################
#### SCRIPT ####
################

my $db = DBI->connect(
    "dbi:mysql:dbname=$p_from{db};host=$p_from{host}",
    $p_from{user},
    $p_from{pass},
    { mysql_enable_utf8 => 1 }
);

my @tables = get_tables($db, $p_from{db});

foreach my $table (@tables) {

  print "\n" . $table->{nome} . " [". $table->{tamanho} ."]";

  if ($table->{nome}) {    
    get_table(\%p_from, \%p_to, $table->{nome}, $limit);
  }

  print " -- done";
}



################
## FUNCTIONS ###
################

sub get_table {
  my %from = %{shift()};
  my %to = %{shift()};
  my $table_name = shift;
  my $limit = shift;

   my $cmd = join (' ',
      'mysqldump',
      '-h',
      $from{host},
      '-u',
      $from{user},
      '-p' . "'$from{pass}'",
      '--opt',
      $from{db},
      $table_name,
      ($limit && $limit =~ /^\d+$/ ? "--where=\"true LIMIT $limit\"" : ''),
      '2>/dev/null',
      '|',
      'mysql',
      '-h',
      $to{host},
      '-P',
      '3306',
      '-u',
      $to{user},
      '-p'."'$to{pass}'",
      $to{db},
      '2>/dev/null',
    );

  if (!$limit) {
    $cmd =~ s/  / /g;
  }

  system($cmd);
  #print($cmd);
}

sub get_tables {
  my $db = shift;
  my $schema = shift;
  # query
  my $query =
    qq{
      SELECT 
        TABLE_NAME as nome,
        ROUND((data_length + index_length) / 1024 / 1024, 2) as tamanho
      FROM
        information_schema.TABLES
      WHERE
        table_schema = '$schema'
      ORDER BY (data_length + index_length) ASC, TABLE_NAME ASC;
  };
  my $stmt = $db->prepare($query);
  $stmt->execute();

  print "loading tables...\n";
  my @tables;
  while(my $row = $stmt->fetchrow_hashref) {
   push @tables, $row; 
  }
  return @tables;
}

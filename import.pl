use strict;
use warnings;

use Getopt::Long;
use DBI;
use LWP;
use JSON;
use POSIX ":sys_wait_h";

my $start_time = time();

# Process command line
my ($tenant,$storage_url,$db_credentials);
my $help = 0;
my $forks = 1;
my $rules = 'rules.json';
my $drop_indexes = 0;
my $analyze = 0;

GetOptions( 'h' => \$help,
            't=s' => \$tenant,
            'u=s' => \$storage_url,
            'f=i' => \$forks,
            'r=s' => \$rules,
            'drop-indexes' => \$drop_indexes,
            'analyze' => \$analyze,
            'db-credentials=s' => \$db_credentials );

my $data_loader = pop(@ARGV);
my @import = @ARGV;

if ($help) {
  help();
  exit;
}
unless ($tenant) {
  help();
  die "Missing tenant on command line\n";
}
unless ($storage_url) {
  help();
  die "Missing storage URL on command line\n";
}
unless ($forks > 0) {
  help();
  die "Invalid number of processes specified on command line\n";
}
if ($drop_indexes || $analyze) {
  unless ($db_credentials) {
    help();
    die "DB credentials not specified on command line\n";
  }
}
unless ($data_loader) {
  help();
  die "No data loader URL specified on command line\n";
}
unless (@import) {
  help();
  die "No import directories specified on command line\n";
}

# Gather files into arrays for forking
my @import_files;
foreach my $dir (@import) {
  unless (-d $dir) {
    help();
    die "$dir is not a directory.\n";
  }
  opendir(IMPORTDIR,$dir)
    or die "Can't open import directory $dir: $!\n";
  my @files = readdir(IMPORTDIR);
  closedir(IMPORTDIR);
  foreach my $file (@files) {
    if ($file ne '.' && $file ne '..') {
      push(@import_files,("$dir/$file"));
    }
  }
}

my @process_files;
my $i = 0;
foreach my $file (@import_files) {
  if ($i == $forks) {
    $i = 0;
  }
  if ($process_files[$i]) {
    push(@{$process_files[$i]},($file));
  } else {
    $process_files[$i] = [ $file ];
  }
  $i++;
}

# Post rules file
print "Posting rules file...";
my $rules_raw = slurp($rules);
my $ua = LWP::UserAgent->new;
my $resp = $ua->post("$data_loader/load/marc-rules",
                     'Accept' => 'text/plain',
                     'Content-Type' => 'application/octet-stream',
                     'X-Okapi-Tenant' => $tenant,
                     Content => $rules_raw);
if (!$resp->is_success) {
  die "error posting rules file: " . $resp->status_line . "\n";
} else {
  print $resp->status_line . "\n";
}

# Get DBH
my $dbh;
if ($analyze || $drop_indexes) {
  my $db_credentials_json = slurp($db_credentials);
  my $db_credentials_ref = decode_json($db_credentials_json);
  print "Connecting to db...";
  $dbh = DBI->connect("dbi:Pg:dbname=$$db_credentials_ref{database};host=$$db_credentials_ref{host};port=$$db_credentials_ref{port}",$$db_credentials_ref{username},$$db_credentials_ref{password})
    or die "Error connecting to database: $DBI::errstr\n";
  print "OK\n";
}

# Drop indexes (save for reinstating after load)
my $indexes;
if ($drop_indexes) {
  print "Dropping indexes pre-import...\n";
  $indexes = $dbh->selectall_arrayref("SELECT indexname, indexdef FROM pg_indexes WHERE schemaname = '${tenant}_mod_inventory_storage' AND tablename = 'instance'");
  if ($indexes && @{$indexes}) {
    foreach my $i (@{$indexes}) {
      if ($$i[0] ne 'instance_pkey') {
        print "indexdef: $$i[1]\n";
        $dbh->do("DROP INDEX ${tenant}_mod_inventory_storage.$$i[0]")
          or warn "Unable to drop index ${tenant}_mod_inventory_storage.$$i[0]: $DBI::errstr\n";
      }
    }
  } else {
    warn "no indexes found on table ${tenant}_mod_inventory_storage.instance\n";
  }
}

# Fork processes for record load
print "Loading MARC files...\n";
if ($forks > 1) {
  for (my $i = 0; $i < $forks; $i++) {
    my $pid = fork();
    if (!defined($pid)) {
      warn "Unable to fork: $!\n";
    } elsif ($pid == 0) {
      load_records(@{$process_files[$i]});
      exit;
    } else {
      print "Launched $pid to process files\n";
    }
  }
  my $kid;
  do {
    $kid = waitpid(-1,&WNOHANG);
  } until $kid == -1;
  print "All file processing complete\n";
} else {
  load_records(@{$process_files[0]});
}

# Reinstate indexes
if ($indexes && @{$indexes}) {
  print "Rebuilding indexes...\n";
  foreach my $i (@{$indexes}) {
    if ($$i[0] ne 'instance_pkey') {
      print "\t rebuilding $$i[0]...";
      if ($dbh->do($$i[1])) {
        print "done\n";
      } else {
        warn "Unable to reinstate index ${tenant}_mod_inventory_storage.$$i[0]: $DBI::errstr\nIndex def: $$i[1]\n";
      }
    }
  }
}

# Analyze table
if ($analyze) {
  print "Analyzing table...";
  if ($dbh->do("VACUUM ANALYZE ${tenant}_mod_inventory_storage.instance")) {
    print "done\n";
  } else {
    warn "table analyze failed: $DBI::errstr\n";
  }
}

my $total_duration = time() - $start_time;
print "Record load completed in $total_duration secs.\n";

exit;

sub load_records {
  my @import_files = @_;
  my $req = HTTP::Request->new( 'POST',"$data_loader/load/marc-data?storageURL=$storage_url",
                                [ 'Accept' => 'text/plain',
                                  'Content-Type' => 'application/octet-stream',
                                  'X-Okapi-Tenant' => $tenant ] );
  foreach my $i (@import_files) {
    my $start = time();
    my $marc = slurp($i);
    $req->content($marc);
    my $resp = $ua->request($req);
    my $duration = time() - $start;
    if ($resp->is_success) {
      my $result = $resp->header('X-Unprocessed');
      if ($resp->header('errors')) {
        $result .= ' ' . $resp->header('errors');
      }
      print "Processed $i: $result ($duration sec.)\n";
    } else {
      warn "Record load failed for $i: " . $resp->status_line . "\n";
    }
  }
}

sub slurp {
  my $file = shift;
  open my $fh, '<', $file or die "Unable to open $file: $!\n";
  local $/ = undef;
  my $cont = <$fh>;
  close $fh;
  return $cont;
}

sub help {
  print <<EOF;
Usage

perl import.pl [-h] -t tenant_id -u storage_URL [-f number_of_processes] [-r rules_file] [--drop-indexes] [--analyze] [--db-credentials db_credentials_file] import_directory [import_directory...] data_loader_URL

Options

-h : Print help message.

-t : The tenant ID under which to load the records (required).

-u : The storageURL parameter to pass to the test-data-loader --
     URL for mod-inventory-storage (required).

-f : Number of processes to fork for running the import in parallel
     (optional, default no forking).

-r : Path to a JSON file of conversion rules to post to the
     test-data-loader (optional, default "rules.json" in current
     working directory).

--analyze : Perform a "VACUUM ANALYZE" of the mod-inventory-storage
            "instance" table after data load (optional, if used
            requires --db-credentials option).

--drop-indexes : Drop indexes before performing data load, recreate
                 after (optional, if used requires --db-credentials option).
                 WARNING: Do not use this option on a production database,
                 as it will severely affect search and sort performance on
                 the "instance" table!

--db-credentials : Path to a file containing database credentials (optional,
                   but required for --drop-indexes or --analyze options).

Arguments

import_directory : Path to a directory containing MARC binary files
                   (in UTF-8 format). All files in the directory will
                   be loaded. It is recommended that individual files
                   contain no more than 50,000 records.

data_loader_url : URL for the test-data-loader.

Database credentials

The db_credentials_file is a simple JSON file using the following format:

{
  "host": "postgres.example.com",
  "port": 5432,
  "username": "folio_user",
  "password": "mysecretpassword",
  "database": "inventory_storage_database"
}

The values, of course, depend on your particular installation.
EOF
}

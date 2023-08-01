#!/usr/bin/perl

use strict;
use warnings;

my %hash_exmp = (
    key_1 => "file1",
    key_2 => "file2",
    key_3 => "file3",
);

my $testkey = 'key_1';
print "$hash_exmp{$testkey}\n";

my $timerange = '["Fri Oct 01 00:00:00 2010 PDT","Wed Oct 06 00:00:00 2010 PDT")';
my @split_time = split(',',substr($timerange, 1, -1));
# print $split_time[1];

EQ: foreach my $ts (@split_time) {
    print "byebye\n";
    last EQ;
    print "$ts\n";
}

my @keys = keys %hash_exmp;

foreach my $key (@keys) {
    print "$key\n";
}
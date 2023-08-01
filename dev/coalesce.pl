    my ($p_query, $p_list_of_fields_ref, $p_temporal_field) = @_;

    my @list_of_fields = @{$p_list_of_fields_ref};
    my $fields_string = join(",", map({ "\"$_\"" } @list_of_fields));

    my $query = "$p_query ORDER BY $fields_string, \"$p_temporal_field\"";
    my $sth = spi_query($query);

    my $curr_row;
    my $to_coalesce = undef;
    my $count = 0;
    my $coalesce_start = undef;
    my $coalesce_end = undef;
    my @endpoints = undef;

    while (defined ($curr_row = spi_fetchrow($sth))) {
        # elog(NOTICE, "curr_row = $curr_row to_coalesce  = $to_coalesce");
        my @endpoints = split(',',substr($curr_row->{$p_temporal_field}, 1, -1));

        if(!(defined $to_coalesce)) {
            # elog(NOTICE, "HELLO HELLO FIRST LOOP");
            $to_coalesce = $curr_row;
            # elog(NOTICE, "to_coalesce  = $to_coalesce");
            $coalesce_start = $endpoints[0];
            $coalesce_end = $endpoints[1];
        } else {
            my $equal = 1;
            ISEQ: foreach my $field (@list_of_fields) {
                if ($curr_row->{$field} ne $to_coalesce->{$field}) {
                    $equal = 0;
                    last ISEQ;
                }
            }

            if ($equal && ($coalesce_end eq $endpoints[0])) {
                $coalesce_end = $endpoints[1];
            } else {
                $to_coalesce->{$p_temporal_field} = "[$coalesce_start,$coalesce_end)";
                return_next($to_coalesce);

                $to_coalesce = $curr_row;
                $coalesce_start = $endpoints[0];
                $coalesce_end = $endpoints[1];
            }
        }
    }

    $to_coalesce->{$p_temporal_field} = "[$coalesce_start,$coalesce_end)";
    return_next($to_coalesce);

    return undef;
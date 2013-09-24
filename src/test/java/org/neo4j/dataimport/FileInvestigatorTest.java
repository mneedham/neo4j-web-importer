package org.neo4j.dataimport;

import org.junit.Test;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

public class FileInvestigatorTest
{
    @Test
    public void shouldDetermineWhichFileSeparatorIsMostLikely()
    {
        assertThat( FileInvestigator.mostLikelySeparator( "mark,ian,jim" ).getKey(), equalTo(","));
        assertThat( FileInvestigator.mostLikelySeparator( "mark\tian\tjim" ).getKey(), equalTo("\\t"));
    }

    @Test
    public void shouldDetermineWhichFileTypeIsMostLikely()
    {
        assertThat( FileInvestigator.mostLikelyFileType( "id,ian,jim" ).first(), equalTo(FileType.NODES_COMMA_DELIMITED_CSV ));
        assertThat( FileInvestigator.mostLikelyFileType( "id\tian\tjim" ).first(), equalTo(FileType.NODES_TAB_DELIMITED_CSV ));

        assertThat( FileInvestigator.mostLikelyFileType( "from\tto\ttype" ).first(), equalTo(FileType.RELATIONSHIPS_TAB_DELIMITED_CSV ));
        assertThat( FileInvestigator.mostLikelyFileType( "from,to,type" ).first(), equalTo(FileType.RELATIONSHIPS_COMMA_DELIMITED_CSV ));
    }
}

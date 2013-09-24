package org.neo4j.dataimport;


public enum FileType
{
    NODES_TAB_DELIMITED_CSV("Tab delimited nodes CSV file", '\t'),
    NODES_COMMA_DELIMITED_CSV( "Comma delimited nodes CSV file", ','),
    RELATIONSHIPS_TAB_DELIMITED_CSV( "Tab delimited relationships CSV file", '\t'),
    RELATIONSHIPS_COMMA_DELIMITED_CSV("Comma delimited relationships CSV file" , ',' );

    private String friendlyName;
    private Character separator;

    FileType( String friendlyName, Character separator )
    {
        this.friendlyName = friendlyName;
        this.separator = separator;
    }


    public String friendlyName()
    {
        return friendlyName;
    }

    public Character separator()
    {
        return separator;
    }
}

# Sheets-Tool
This python utility grabs a tab from a google sheet using an API key, transposes the data, and saves it as a CSV on the local machine
It was created to solve a problem when doing vMix broadcasts where you'll run into API limits within vMix itself. With this tool you can grab a new CSV every second and never time out the API. 

Why does the data transpose? I'm glad you asked.
vMix has a feature where it can use first columns as headers, but often laying out your data like that leads to a very WIDE spreadsheet without much past row 5 or 6.
By transposing the data, you are able to use the first column in each row as a header effectively. So on my vMix output sheet I have my data laid out like this:

1       2        3       4        5
2 Playername   Savage   Pete     Jim
3 Casters      Jim     gamer12   WDH
4 Caster_&     @Jim     @G12     @QDH


This way when I select my data in vMix, it's all labeled, and the layout in google sheets is somewhat sane.
When I select a data source in vMix I just select "Casters" and then the row number depending on which caster I want. Caster 1? Row 1, etc.

So far this system has been very reliable.

# Sheets-Tool
This python utility grabs a tab from a google sheet using an API key, transposes the data, and saves it as a CSV on the local machine.

It was created to solve a problem when doing vMix broadcasts where you'll run into API limits within vMix itself. With this tool you can grab a new CSV every second and never time out the API. 

Why does the data transpose? I'm glad you asked.
vMix has a feature where it can use first columns as headers, but often laying out your data like that leads to a very W I D E spreadsheet without much past row 5 or 6.
By transposing the data, you are able to use the first column in each row as a header effectively. So on my vMix output sheet I have my data laid out like this:

| Column 1 | Column 2 | Column 3 | Column 4 |
| -------- | -------- | -------- | -------- |
|Playername| Savage   | Pete     | Jim      |
|Casters   | Jim      | gamer12  |  QDH     |
|Caster_@  | @Jim     | @G12     | @QDH     |


This way when I select my data in vMix, it's all labeled, and the layout in google sheets is somewhat sane.
When I select a data source in vMix I just select "Casters" and then the row number depending on which caster I want. Caster 1? Row 1, etc.

In this case, if I want to select Jim on my data source, I select casters from my dropdown list, and select row 1. Remember, after it gets transposed, the first columns act as headers. So what looks like row 2, is actually row 1. Just think of it like it's the first piece of data after the label. Data 1.

So far this system has been very reliable.

# Dependencies for users to install if running from python file.

requests: pip install requests

pandas: pip install pandas

tkinter: pip install tkinter

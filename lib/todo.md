keep refactoring app.js

follow the logic path.  have detector freeways, have hpms groupings.
So remove the freeways that I have in calvad from the hpms totals

then do the pull from couchdb, and multiply the aadt by hourly factors

1. pull from couchdb hpms

2. pull from couchdb detector data

3. merge hpms and detector data, handling "duplicate" freeways

3. sql query for hpms not assigned geo inside county

4. sql query for grid count of assigned geo inside county

5. evenly distribute hpms not assigned to grids in county

6. modify 3 to include 5

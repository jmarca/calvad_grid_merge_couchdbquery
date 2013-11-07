keep refactoring app.js

follow the logic path.  have detector freeways, have hpms groupings.
So remove the freeways that I have in calvad from the hpms totals

then do the pull from couchdb, and multiply the aadt by hourly factors

3. sql query for hpms not assigned geo inside county

4. sql query for grid count of assigned geo inside county

5. evenly distribute hpms not assigned to grids in county

6. modify 3 to include 5

7. save the cumulative sums to couchdb or something by county or something

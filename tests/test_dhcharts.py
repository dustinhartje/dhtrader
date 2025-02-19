import site
site.addsitedir('modulepaths')
import dhcharts as dhc
import dhutil as dhu
from dhutil import dt_as_dt, dt_as_str

# TODO think through which tests can be done simply by creating and calcing,
#      and which should pull data from storage to confirm live results
#      Probably many should have both.  Should they be in the same file?
# TODO Consider creating a list of tested attributes and methods for each class
#      and then checking this against a list of an instance's actual attributes
#      and methods.  If any are found that aren't in the list raise an error
#      so that it reminds me to add/update tests when I add new stuff
#      This should be an easily copyied function at the top of each file
#      or possibly it's own file I can import to keep it DRY.  Maybe all
#      the other shared test functions should be in this hypothetical test
#      library file as well including create_*?
# TODO check all test files for hide_ functions I forgot to unhide
# TODO confirm no other TODOs remain in this file before clearing this one

# TODO Tests needed (some of these have already been written partially/fully
# bot() returns correct date

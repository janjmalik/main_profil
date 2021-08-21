using System;
using System.Collections.Generic;
using System.IO;

namespace metastrings
{
    public static class TestUtils
    {
        public static Context GetCtxt()
        {
            return new Context("Data Source=[UserRoaming]/metastrings-tests.db");
        }
    }
}

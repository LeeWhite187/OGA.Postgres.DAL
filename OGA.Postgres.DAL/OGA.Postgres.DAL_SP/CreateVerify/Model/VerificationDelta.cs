using System;
using System.Collections.Generic;
using System.Text;

namespace OGA.Postgres.DAL_SP.CreateVerify.Model
{
    /// <summary>
    /// Represents a single difference between a layout and a live database object.
    /// </summary>
    public class VerificationDelta
    {
        public eObjType ObjType { get; set; }
        public string ObjName { get; set; }
        public string ParentName { get; set; }

        public eErrorType ErrorType { get; set; }

        public string ErrText { get; set; }

    }
}

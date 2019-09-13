﻿// $Id$
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Globalization;

namespace Ookii.Jumbo.Jet.Samples.FPGrowth
{
    struct FPTreeNode
    {
        public int Parent;
        public int Id;
        public int Count;
        public int NodeLink;
        public int Copy;

        //public FPTreeNode GetChild(int id)
        //{
        //    foreach( FPTreeNode child in Children )
        //    {
        //        if( child.Id == id )
        //            return child;
        //    }
        //    return null;
        //}

        public override string ToString()
        {
            return string.Format(CultureInfo.InvariantCulture, "{0}:{1}", Id, Count);
        }
    }
}

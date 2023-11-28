// Copyright (c) Sven Groot (Ookii.org)
using System;
using System.Runtime.InteropServices;

namespace Ookii.Jumbo.Jet.Samples.FPGrowth;

unsafe struct NodeChildList
{
    private const float _growthRate = 1.5f;

    public int Count;
    public int ChildrenLength;
    public int* Children;

    public void Add(int node)
    {
        int newChild = Count++;
        if (ChildrenLength == 0)
        {
            Children = (int*)Marshal.AllocHGlobal(2 * sizeof(int));
            ChildrenLength = 2;
        }
        else if (ChildrenLength < Count)
        {
            int newSize = (int)(ChildrenLength * _growthRate);
            Children = (int*)Marshal.ReAllocHGlobal((IntPtr)Children, new IntPtr(newSize * sizeof(int)));
            ChildrenLength = newSize;
        }
        Children[newChild] = node;
    }
}

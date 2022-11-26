#!/usr/bin/python
# coding=utf-8
#
# Compare a result file to an expected value

def compare_files(test_file, ref_file):
    ret = True
    ln = 0
    with open(test_file, "rt") as f1:
        with open(ref_file, "rt") as f2:
            while ret:
                ln += 1
                line1 = f1.readline()
                line2 = f2.readline()
                if not line1:
                    if line2:
                        print("expected line " + str(ln) + " is missing from test file " + test_file)
                        ret = False
                    break
                else:
                    line1 = line1.strip()
                    if not line2:
                        print("Extra line " + str(ln) + " in " + test_file + ":\n    " + line1)
                        ret = False
                        break
                    else:
                        line2 = line2.strip()
                        if line1 != line2:
                            print("Test file " + test_file + " differs from " + ref_file + " at line " + str(ln))
                            print("    " + line1 + "\nvs.: \n    " + line2)
                            ret = False
                            break
    return ret



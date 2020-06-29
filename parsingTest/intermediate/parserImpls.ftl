void quux(int arg1,
    int arg2
) :
{
    char x = ' } '
    String y;
}
{
    /* All invalid curly braces:
    }
    // }
    " } "
    ' } '
    */
    <TOKEN> {
        y = " { } } "
    }

    // Not a string: "
}
void baz() : {x}
{
    // overridden by intermediate
}

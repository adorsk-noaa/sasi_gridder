from javax.swing import (Spring, SpringLayout)

def makeGrid(parent, rows,cols, initialX, initialY, xPad, yPad):
    layout = parent.getLayout()
    xPadSpring = Spring.constant(xPad)
    yPadSpring = Spring.constant(yPad)
    initialXSpring = Spring.constant(initialX)
    initialYSpring = Spring.constant(initialY)
    max_ = rows * cols

    #Calculate Springs that are the max of the width/height so that all
    #cells have the same size.
    maxWidthSpring = layout.getConstraints(parent.getComponent(0)).getWidth()
    maxHeightSpring = layout.getConstraints(parent.getComponent(0)).getHeight()
    for i in range(1, max_):
        cons = layout.getConstraints(parent.getComponent(i))
        maxWidthSpring = Spring.max(maxWidthSpring, cons.getWidth())
        maxHeightSpring = Spring.max(maxHeightSpring, cons.getHeight())

    # Apply the new width/height Spring. This forces all the
    # components to have the same size.
    for i in range(1, max_):
        cons = layout.getConstraints(parent.getComponent(i))
        cons.setWidth(maxWidthSpring)
        cons.setHeight(maxHeightSpring)

    # Then adjust the x/y constraints of all the cells so that they
    # are aligned in a grid.
    lastCons = None
    lastRowCons = None
    for i in range(1, max_):
        cons = layout.getConstraints(parent.getComponent(i))
        # start of new row
        if (i % cols == 0):
            lastRowCons = lastCon
            cons.setX(initialXSpring)
        # x position depends on previous component
        else:
            cons.setX(Spring.sum(lastCons.getConstraint(SpringLayout.EAST), 
                                 xPadSpring))
        # first row
        if (i / cols == 0):
            cons.setY(initialYSpring)
        # y position depends on previous row
        else:
            cons.setY(Spring.sum(lastRowCons.getConstraint(SpringLayout.SOUTH),
                                 yPadSpring))
        lastCons = cons

    # Set the parent's size.
    pCons = layout.getConstraints(parent)
    pCons.setConstraint(SpringLayout.SOUTH,
                        Spring.sum(
                            Spring.constant(yPad),
                            lastCons.getConstraint(SpringLayout.SOUTH)))
    pCons.setConstraint(SpringLayout.EAST,
                        Spring.sum(
                            Spring.constant(xPad),
                            lastCons.getConstraint(SpringLayout.EAST)))

def getConstraintsForCell(row, col, parent, cols):
    """ Helper method for makeCompactGrid. """
    layout = parent.getLayout()
    c = parent.getComponent(row * cols + col)
    return layout.getConstraints(c)

def makeCompactGrid(parent, rows, cols, initialX, initialY, xPad, yPad):
    """
    Aligns the first <code>rows</code> * <code>cols</code>
    components of <code>parent</code> in
    a grid. Each component in a column is as wide as the maximum
    preferred width of the components in that column;
    height is similarly determined for each row.
    The parent is made just big enough to fit them all.

    @param rows number of rows
    @param cols number of columns
    @param initialX x location to start the grid at
    @param initialY y location to start the grid at
    @param xPad x padding between cells
    @param yPad y padding between cells
    """
    layout = parent.getLayout()

    # Align all cells in each column and make them the same width.
    x = Spring.constant(initialX)
    for c in range(cols):
        width = Spring.constant(0);
        for r in range(rows):
            width = Spring.max(
                width, getConstraintsForCell(r, c, parent, cols).getWidth())
        for r in range(rows):
            constraints = getConstraintsForCell(r, c, parent, cols)
            constraints.setX(x)
            constraints.setWidth(width)

        x = Spring.sum(x, Spring.sum(width, Spring.constant(xPad)))

    # Align all cells in each row and make them the same height.
    y = Spring.constant(initialY)
    for r in range(rows):
        height = Spring.constant(0)
        for c in range(cols):
            height = Spring.max(
                height, getConstraintsForCell(r, c, parent, cols).getHeight())

        for c in range(cols):
            constraints = getConstraintsForCell(r, c, parent, cols)
            constraints.setY(y)
            constraints.setHeight(height)

        y = Spring.sum(y, Spring.sum(height, Spring.constant(yPad)))

    # Set the parent's size.
    pCons = layout.getConstraints(parent)
    pCons.setConstraint(SpringLayout.SOUTH, y)
    pCons.setConstraint(SpringLayout.EAST, x)

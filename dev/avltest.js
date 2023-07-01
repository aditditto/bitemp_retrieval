const AVLTree = require("./avl");

const date_tree = new AVLTree(null, true);

date_tree.insert(new Date('2010-10-10'));
date_tree.insert(new Date('2010-10-11'));
date_tree.insert(new Date('2010-10-12'));

let count = 1;
date_tree.forEach(node => {
    node.data = [count];
    count++;
});

date_tree.forEach(node => {
    console.log(node.key);
    console.log(node.data);
});
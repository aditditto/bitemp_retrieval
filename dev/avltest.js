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

function pushOne(arr) {
    arr.push(1);
}

console.log("test pushone");
const testarr = [];

pushOne(testarr);

testarr.forEach(el => console.log(el));

console.log("test insert duplicate");
// v ← node in gt[i].T with time v.t = r.Te (insert a new node if required); no duplicates
let node = date_tree.insert(new Date('2010-01-01'), []);
console.log(`node key: ${node.key}`);
node = node === null ? date_tree.find(new Date('2010-01-01')) : node; // v.open ← v.open ∪ r[A1, . . . , Ap, Ts];
node.data.push(1);
console.log(node.data);

node = date_tree.insert(new Date('2010-01-01'), []);
console.log(node);
node = node === null ? date_tree.find(new Date('2010-01-01')) : node; // v.open ← v.open ∪ r[A1, . . . , Ap, Ts];
node.data.push(2);

console.log(node.data);

console.log(date_tree.toString(function(node) {
    return `${node.key.toISOString()} [${node.data}]`
}));

const num_tree = new AVLTree(null, true);
num_tree.insert(12);
num_tree.insert(8);
num_tree.insert(18);
num_tree.insert(5);
num_tree.insert(11);
num_tree.insert(17);
num_tree.insert(4);

console.log(num_tree.toString());
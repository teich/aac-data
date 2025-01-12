## Model

People belong to one company.
Companies can have many people.
Orders belong to one person.
People can have many orders.
Each order has many line items
Line items have a product and a quantity

### Person
- Name [String]
- Email [String]
- Phone [String]
- Address [String]
- City [String]
- State [String]
- Zip [String]
- Country [String]
- Company [CompanyId]

### Companies
- Name [String]
- Domain [String]
- Estimated Revenue [String]
- Linkedin URL [String]
- Size Range [String]
- Employees Count [Integer]
- Industry [String]
- Categories [Array]

### Orders
- Person [PersonId]
- Date [Date]
- Amount [Float]

### Line Items
- Order [OrderId]
- Product [ProductId]
- Unit Price [Float]
- Quantity [Integer]
- Amount [Float]

### Products
- Name [String]
- Description [String]
- SKU [String]
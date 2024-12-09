---
linkTitle: "Writing documentation"
title: "Writing documentation"
weight: 2
---

UCX documentation uses the following set of technologies:

- [Hugo](https://gohugo.io/) - Static site generator written in Go
- [Hextra](https://imfing.github.io/hextra/) - Hugo theme for documentation
- [TailwindCSS](https://tailwindcss.com/) - Utility-first CSS framework, mainly used for custom styling


## Prerequisites

To build the documentation, you need to have the following tools installed:
- [Hugo](https://gohugo.io/getting-started/installing/)
- [Node.js](https://nodejs.org/en/download/)
- [Yarn](https://classic.yarnpkg.com/en/docs/install/)

{{<callout type="info">}}
The documentation is built using Hugo, a static site generator. The content is written in Markdown and the site is styled using TailwindCSS. We only need node.js and yarn to make tailwindcss work.
{{</callout>}}

## Installing tailwindcss

To install the dependencies, run the following commands:

```bash
cd docs && yarn install
```

## Installing Hugo

To install Hugo, follow the instructions on the [Hugo website](https://gohugo.io/getting-started/installing/).

## Running the documentation locally

To run the documentation locally, run the following command:

```bash
cd docs && hugo server
```

Follow the instructions in the terminal to open the documentation in your browser. The default URL is `http://localhost:1313/ucx`.

## Adding static files or images

To add static files or images, place them in the `docs/static` directory. 

{{< callout type="info" >}}
All images, including `.gif` files should be stored in the `docs/static/images` directory.
{{< /callout >}}

You can reference them in your markdown files using the following syntax:

```markdown
![Alt text](/images/image.png)
```

## Using shortcodes

Shortcodes are used to add reusable components to the documentation. You can find the list of available shortcodes in the [Hextra documentation](https://imfing.github.io/hextra/docs/guide/shortcodes/).

We mainly use the following shortcodes:
- [callout](https://imfing.github.io/hextra/docs/guide/shortcodes/callout/)
- [tabs](https://imfing.github.io/hextra/docs/guide/shortcodes/tabs/)
- [steps](https://imfing.github.io/hextra/docs/guide/shortcodes/steps/)




## Writing content

We follow a strict structure for writing content in the documentation. Each markdown file should have the following front matter:

```markdown
---
linkTitle: "Link title"
title: "Title"
weight: 1 # optional, defines the order of the page in the sidebar
---
```

Our documentation is split into the following 4 pieces:

- Installation: guidance on how to install UCX
- Process: guidance on how to use UCX in simple, step-by-step instructions
- Reference: detailed information about UCX features, commands, codes, configurations, etc.
- Developer documentation: anythign related to the development of UCX, as well as technical implementation details


{{< callout type="warning" >}}
Please don't put any technical implementation details in the installation, process, or reference sections. These sections are meant to be user-friendly and easy to understand.
When in doubt, please ask for a review.
{{< /callout >}}

Please make sure (especially in Process and Reference sections) to correctly order the pages in the sidebar. The order should be logical and easy to follow for the user.

Please don't use nesting when not necessary. 3 levels of nesting are maximum. If you need more, please consider restructuring the content.


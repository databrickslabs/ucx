import React from 'react';
import Layout from '@theme/Layout';
import Link from '@docusaurus/Link';

export default function Home(): JSX.Element {
  return (
    <Layout
      title="UCX - Unity Catalog Toolkit"
      description="UCX helps assess, plan, and execute your Databricks Unity Catalog migration efficiently."
    >
      <main className="flex flex-col md:flex-row items-center justify-center px-6 md:px-12 py-12">
        <div className="md:w-2/3 text-center md:text-left">
          <h1 className="text-4xl font-bold mb-6">
            Simplify and Accelerate your Unity Catalog Migration
          </h1>
          <p className="text-lg mb-8">
            The open-source tool that helps assess, plan, and execute your Databricks Unity Catalog migration with confidence.
          </p>
          <div className="flex justify-center md:justify-start space-x-4">
            <Link
              className="px-6 py-3 text-white bg-red-600 rounded-md hover:bg-red-700 hover:text-white transition-colors duration-200"
              to="/docs/gettingstarted"
            >
              Get Started
            </Link>
          </div>
        </div>

        <div className="hidden md:block md:w-1/3 mt-10 md:mt-0">
          <img
            src="img/logo.svg"
            alt="UCX Logo"
            className="w-2/3 mx-auto md:w-full md:max-w-xs"
          />
        </div>
      </main>

    <section className="px-6 md:px-12 py-12 text-center bg-gray-50 dark:bg-gray-900 text-gray-900 dark:text-gray-100">
      <h2 className="text-3xl font-semibold mb-8">Key Features</h2>
      <div className="flex flex-col md:flex-row justify-center space-y-8 md:space-y-0 md:space-x-12">
        <div>
          <h3 className="text-xl font-bold mb-2">🔍 Comprehensive Assessment</h3>
          <p>Identify migration blockers and readiness with detailed analysis.</p>
        </div>
        <div>
          <h3 className="text-xl font-bold mb-2">🚀 Automated Migrations</h3>
          <p>Simplify moving data assets and permissions efficiently.</p>
        </div>
        <div>
          <h3 className="text-xl font-bold mb-2">📊 Detailed Reporting</h3>
          <p>Gain actionable insights into your migration progress.</p>
        </div>
      </div>
    </section>


      <footer className="text-center py-6 border-t mt-12 text-gray-600 text-sm">
        <a href="https://github.com/databrickslabs/ucx" target="_blank" rel="noopener" className="mx-2 hover:underline">GitHub</a>|
        <Link to="docs/dev" className="mx-2 hover:underline">Contribute</Link>
      </footer>
    </Layout>
  );
}

// lib/suggestions.ts

export type Suggestion = {
  prompt: string;
  title: string;
  description?: string;
};

export const SUGGESTIONS: Suggestion[] = [
  {
    prompt: "Which conditions have the highest opportunity score, and what drives that score?",
    title: "Top Opportunity Conditions",
    description: "Identify high-opportunity conditions and key contributing factors",
  },
  {
    prompt: "Which Tier 1 (flagship) conditions should we prioritize for our next sales campaign?",
    title: "Priority Conditions",
    description: "Find top conditions to target for upcoming sales efforts",
  },
  {
    prompt: "Which specialty domains — e.g. cardiology vs. pulmonology — represent the largest untapped Medicare market?",
    title: "Untapped Markets",
    description: "Explore specialty areas with highest growth potential",
  },
  {
    prompt: "Which conditions have growing beneficiary volume but below-average allowed amounts, indicating underserved demand?",
    title: "Underserved Demand",
    description: "Detect conditions with growth but low reimbursement",
  },
  {
    prompt: "Which states have the highest allowed amounts per patient for a given condition?",
    title: "High-Value States",
    description: "Analyze states with highest reimbursement per patient",
  }
];